#include "EventQueue.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>

#include <Python.h>

namespace disco {

namespace {

// Safe GIL guard for refcount ops (also works when called from non-Python threads).
class GilGuard {
public:
    GilGuard() : active_(Py_IsInitialized() != 0), state_() {
        if (active_) {
            state_ = PyGILState_Ensure();
        }
    }
    ~GilGuard() {
        if (active_) {
            PyGILState_Release(state_);
        }
    }

    GilGuard(const GilGuard&) = delete;
    GilGuard& operator=(const GilGuard&) = delete;

private:
    bool active_;
    PyGILState_STATE state_;
};

inline void incref(PyObject* obj) {
    if (!obj) return;
    GilGuard gil;
    // If Python is not initialized, gil.active_ is false and this is a no-op.
    if (Py_IsInitialized() != 0) Py_INCREF(obj);
}

inline void decref(PyObject* obj) {
    if (!obj) return;
    GilGuard gil;
    if (Py_IsInitialized() != 0) Py_DECREF(obj);
}

}  // namespace

// -------------------- Event --------------------

Event::Event() noexcept : sender(), epoch(0.0), data(nullptr), headers() {}

Event::Event(std::string sender_,
             double epoch_,
             PyObject* data_,
             std::map<std::string, std::string> headers_,
             BorrowedRefTag)
    : sender(std::move(sender_)),
      epoch(epoch_),
      data(data_),
      headers(std::move(headers_)) {
    incref(data);
}

Event::Event(std::string sender_,
             double epoch_,
             PyObject* data_,
             std::map<std::string, std::string> headers_,
             OwnedRefTag) noexcept
    : sender(std::move(sender_)),
      epoch(epoch_),
      data(data_),
      headers(std::move(headers_)) {}

Event::Event(const Event& other)
    : sender(other.sender),
      epoch(other.epoch),
      data(other.data),
      headers(other.headers) {
    incref(data);
}

Event& Event::operator=(const Event& other) {
    if (this == &other) return *this;
    decref(data);
    sender = other.sender;
    epoch = other.epoch;
    data = other.data;
    headers = other.headers;
    incref(data);
    return *this;
}

Event::Event(Event&& other) noexcept
    : sender(std::move(other.sender)),
      epoch(other.epoch),
      data(other.data),
      headers(std::move(other.headers)) {
    other.data = nullptr;
}

Event& Event::operator=(Event&& other) noexcept {
    if (this == &other) return *this;
    decref(data);
    sender = std::move(other.sender);
    epoch = other.epoch;
    data = other.data;
    headers = std::move(other.headers);
    other.data = nullptr;
    return *this;
}

Event::~Event() {
    decref(data);
    data = nullptr;
}

PyObject* Event::release_data() noexcept {
    PyObject* out = data;
    data = nullptr;
    return out;
}

// -------------------- EventQueue --------------------

EventQueue::EventQueue()
    : _predecessors(),
      _epoch(-1.0),
      _next_epoch(NAN),
      _waiting_for() {}

double EventQueue::getEpoch() const {
    std::lock_guard<std::mutex> lock(_mtx);
    return _predecessors.empty() ? INFINITY : _epoch;
}

double EventQueue::getNextEpoch() const {
    std::lock_guard<std::mutex> lock(_mtx);
    return _predecessors.empty() ? INFINITY : _next_epoch;
}

const std::string& EventQueue::getWaitingFor() const {
    std::lock_guard<std::mutex> lock(_mtx);
    return _waiting_for;
}

bool EventQueue::hasPredecessors() const {
    std::lock_guard<std::mutex> lock(_mtx);
    return !_predecessors.empty();
}

void EventQueue::registerPredecessor(const std::string& predecessor) {
    std::lock_guard<std::mutex> lock(_mtx);

    if (_predecessors.find(predecessor) != _predecessors.end()) {
        throw std::runtime_error("Predecessor exists: " + predecessor);
    }
    if (_epoch != -1.0) {
        throw std::runtime_error("Predecessors cannot be registered after initialization.");
    }

    _predecessors.emplace(predecessor, std::make_unique<PredecessorEventQueue>());
}

bool EventQueue::empty() const {
    std::lock_guard<std::mutex> lock(_mtx);

    for (const auto& kvp : _predecessors) {
        const auto& pred = kvp.second;
        if (pred->getEpoch() == _epoch && !pred->empty()) {
            return false;
        }
    }
    return true;
}

std::vector<EventQueue::PredSnapshot> EventQueue::snapshotPredecessorsUnlocked() const {
    std::vector<PredSnapshot> out;
    out.reserve(_predecessors.size());
    for (const auto& kvp : _predecessors) {
        const auto& pred = kvp.second;
        out.push_back(PredSnapshot{
            kvp.first,
            pred->getEpoch(),
            pred->getNextEpoch(),
            pred->empty(),
        });
    }
    return out;
}

bool EventQueue::tryNextEpoch() {
    std::lock_guard<std::mutex> lock(_mtx);
    return tryNextEpochUnlocked();
}

bool EventQueue::tryNextEpochUnlocked() {
    if (_predecessors.empty()) {
        return false;
    }

    auto snaps = snapshotPredecessorsUnlocked();

    // Comparator must live inside EventQueue scope because PredSnapshot is private.
    std::sort(snaps.begin(), snaps.end(),
              [](const PredSnapshot& a, const PredSnapshot& b) {
                  // Same ordering as original:
                  // - lowest epoch first
                  // - if equal epoch: non-empty comes first
                  if (a.epoch < b.epoch) return true;
                  if (a.epoch > b.epoch) return false;
                  return (!a.empty && b.empty);
              });

    auto it = snaps.begin();

    double epoch = it->epoch;
    bool epoch_empty = it->empty;
    double next_epoch = it->next_epoch;

    if (std::isnan(next_epoch)) {
        _waiting_for = it->name + " (promises)";
    } else if (epoch_empty) {
        _waiting_for = it->name + " (events)";
    } else {
        _waiting_for.clear();
    }

    ++it;

    while (it != snaps.end() && !std::isnan(next_epoch)) {
        const double pred_epoch = it->epoch;

        if (pred_epoch < next_epoch) {
            const double pred_next_epoch = it->next_epoch;
            const bool pred_empty = it->empty;

            if (epoch_empty) {
                epoch = pred_epoch;
                epoch_empty = pred_empty;
            }

            if (std::isnan(pred_next_epoch)) {
                next_epoch = NAN;
                _waiting_for = it->name + " (promises)";
            } else if (pred_next_epoch < next_epoch) {
                next_epoch = pred_next_epoch;
                _waiting_for = it->name + " (events)";
            }

            ++it;
        } else {
            break;
        }
    }

    if (_epoch != epoch || _next_epoch != next_epoch) {
        _epoch = epoch;
        _next_epoch = next_epoch;
        return true;
    }
    return false;
}

bool EventQueue::push(const std::string& sender,
                      double epoch,
                      PyObject* data,
                      std::map<std::string, std::string>& headers) {
    std::lock_guard<std::mutex> lock(_mtx);

    auto it = _predecessors.find(sender);
    if (it == _predecessors.end()) {
        throw std::runtime_error("Predecessor not found: " + sender);
    }

    const double pred_epoch = it->second->getEpoch();
    const bool updated = it->second->push(epoch, data, headers);

    if (updated && pred_epoch <= _epoch) {
        return tryNextEpochUnlocked();
    }
    return false;
}

bool EventQueue::promise(const std::string& sender,
                         unsigned long seqnr,
                         double epoch,
                         unsigned long num_events) {
    std::lock_guard<std::mutex> lock(_mtx);

    auto it = _predecessors.find(sender);
    if (it == _predecessors.end()) {
        throw std::runtime_error("Predecessor not found: " + sender);
    }

    const double pred_epoch = it->second->getEpoch();
    const bool updated = it->second->promise(seqnr, epoch, num_events);

    if (updated && pred_epoch <= _epoch) {
        return tryNextEpochUnlocked();
    }
    return false;
}

std::vector<Event> EventQueue::pop() {
    std::lock_guard<std::mutex> lock(_mtx);

    std::vector<Event> result;

    for (auto& kvp : _predecessors) {
        auto& pred = kvp.second;
        std::vector<PredecessorEvent> events = pred->pop();

        for (auto& e : events) {
            // Transfer ownership of the Python ref from PredecessorEvent -> Event.
            PyObject* data = e.release_data();
            result.emplace_back(
                kvp.first,
                e.epoch,
                data,
                std::move(e.headers),
                owned_ref
            );
        }
    }

    tryNextEpochUnlocked();
    return result;
}

std::vector<Event> EventQueue::popAll() {
    std::lock_guard<std::mutex> lock(_mtx);

    std::vector<Event> result;

    for (auto& kvp : _predecessors) {
        auto& pred = kvp.second;
        std::vector<PredecessorEvent> events = pred->popAll();

        for (auto& e : events) {
            PyObject* data = e.release_data();
            result.emplace_back(
                kvp.first,
                e.epoch,
                data,
                std::move(e.headers),
                owned_ref
            );
        }
    }

    tryNextEpochUnlocked();
    return result;
}

}  // namespace disco
