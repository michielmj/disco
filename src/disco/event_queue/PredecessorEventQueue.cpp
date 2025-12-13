#include "PredecessorEventQueue.h"

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
    if (Py_IsInitialized() == 0) return;
    GilGuard gil;
    Py_INCREF(obj);
}

inline void decref(PyObject* obj) {
    if (!obj) return;
    if (Py_IsInitialized() == 0) return;
    GilGuard gil;
    Py_DECREF(obj);
}

}  // namespace

// -------------------- PredecessorEvent --------------------

PredecessorEvent::PredecessorEvent() noexcept : epoch(0.0), data(nullptr), headers() {}

PredecessorEvent::PredecessorEvent(double epoch_,
                                   PyObject* data_,
                                   std::map<std::string, std::string> headers_)
    : epoch(epoch_), data(data_), headers(std::move(headers_)) {
    // data_ is BORROWED -> take our own reference
    incref(data);
}

PredecessorEvent::PredecessorEvent(const PredecessorEvent& other)
    : epoch(other.epoch), data(other.data), headers(other.headers) {
    incref(data);
}

PredecessorEvent& PredecessorEvent::operator=(const PredecessorEvent& other) {
    if (this == &other) return *this;

    decref(data);
    epoch = other.epoch;
    data = other.data;
    headers = other.headers;
    incref(data);

    return *this;
}

PredecessorEvent::PredecessorEvent(PredecessorEvent&& other) noexcept
    : epoch(other.epoch), data(other.data), headers(std::move(other.headers)) {
    other.data = nullptr;
}

PredecessorEvent& PredecessorEvent::operator=(PredecessorEvent&& other) noexcept {
    if (this == &other) return *this;

    decref(data);
    epoch = other.epoch;
    data = other.data;
    headers = std::move(other.headers);
    other.data = nullptr;

    return *this;
}

PredecessorEvent::~PredecessorEvent() {
    decref(data);
    data = nullptr;
}

PyObject* PredecessorEvent::release_data() noexcept {
    PyObject* out = data;
    data = nullptr;
    return out;
}

// -------------------- PredecessorEventQueue --------------------

PredecessorEventQueue::PredecessorEventQueue()
    : _event_count(),
      _num_events(),
      _epochs(),
      _events(),
      _epoch(-1.0),
      _next_epoch(NAN),
      _seqnr(0UL) {}

bool PredecessorEventQueue::push(double epoch, PyObject* data, std::map<std::string, std::string>& headers) {
    std::lock_guard<std::mutex> lock(_mtx);

    if (epoch <= _epoch) {
        throw std::runtime_error("Events must be in the future for the predecessor.\n" + dump());
    }

    // PredecessorEvent takes a BORROWED PyObject* and INCREFs it.
    PredecessorEvent new_event(epoch, data, headers);
    _events.push(new_event);

    return _increment(epoch);
}

std::vector<PredecessorEvent> PredecessorEventQueue::pop() {
    std::lock_guard<std::mutex> lock(_mtx);

    std::vector<PredecessorEvent> result;
    const double current_epoch = _epoch;

    while (!_events.empty() && _events.top().epoch == current_epoch) {
        // Copying increments refcount; popping destroys original and decrefs => net: 1 ref held in result.
        result.emplace_back(_events.top());
        _events.pop();
    }

    _tryNextEpoch();
    return result;
}

std::vector<PredecessorEvent> PredecessorEventQueue::popAll() {
    std::lock_guard<std::mutex> lock(_mtx);

    std::vector<PredecessorEvent> result;

    while (!_events.empty()) {
        result.emplace_back(_events.top());
        _events.pop();
    }

    return result;
}

bool PredecessorEventQueue::_increment(double epoch) {
    auto it = _event_count.find(epoch);
    if (it == _event_count.end()) {
        _event_count[epoch] = 1;
    } else {
        it->second++;
    }

    if (epoch == _next_epoch) {
        return _tryNextEpoch();
    }
    return false;
}

double PredecessorEventQueue::epochBefore(unsigned long seqnr) {
    auto it = _epochs.lower_bound(seqnr);
    if (it == _epochs.begin()) {
        return -1.0;
    }
    --it;
    return it->second;
}

double PredecessorEventQueue::epochAfter(unsigned long seqnr) {
    auto it = _epochs.upper_bound(seqnr);
    if (it == _epochs.end()) {
        return INFINITY;
    }
    return it->second;
}

bool PredecessorEventQueue::tryNextEpoch() {
    std::lock_guard<std::mutex> lock(_mtx);
    return _tryNextEpoch();
}

bool PredecessorEventQueue::_tryNextEpoch() {
    bool updated = false;

    if (_epoch == -1.0 && _num_events.find(1UL) != _num_events.end() && _epochs[1UL] > 0.0) {
        _epoch = 0.0;
        updated = true;
    }

    unsigned long try_seqnr = _seqnr + 1UL;

    while ((_events.empty() || _events.top().epoch > _epoch) &&
           (_num_events.find(try_seqnr) != _num_events.end())) {

        const double try_epoch = _epochs.find(try_seqnr)->second;
        const unsigned long num_events = _num_events.find(try_seqnr)->second;

        auto it_cnt = _event_count.find(try_epoch);
        if (it_cnt == _event_count.end()) {
            // promise() should have created an entry (possibly 0).
            break;
        }
        const unsigned long event_count = it_cnt->second;

        if (event_count == num_events) {
            // clean up current epoch metadata
            if (_seqnr > 0UL) {
                _event_count.erase(try_epoch);
                _num_events.erase(_seqnr);
                _epochs.erase(_seqnr);
            }

            // update epoch
            _seqnr = try_seqnr;
            _epoch = try_epoch;

            // update next epoch
            auto it_epoch = _epochs.find(try_seqnr + 1UL);
            if (it_epoch != _epochs.end()) {
                _next_epoch = it_epoch->second;
            } else {
                _next_epoch = NAN;
            }

            ++try_seqnr;
            updated = true;
        } else {
            break;
        }
    }

    return updated;
}

bool PredecessorEventQueue::promise(unsigned long seqnr, double epoch, unsigned long num_events) {
    std::lock_guard<std::mutex> lock(_mtx);

    if (seqnr <= _seqnr) {
        throw std::runtime_error("Seqnr must be greater than the current seqnr of the queue.");
    }

#ifdef DEBUG_ENABLED
    if (epoch <= epochBefore(seqnr)) {
        throw std::runtime_error("Epoch must be greater than the epochs of earlier promises.");
    }
    if (epoch >= epochAfter(seqnr)) {
        throw std::runtime_error("Epoch must be smaller than the epochs of later promises.");
    }
    auto it_repr = _num_events.find(seqnr);
    if (it_repr != _num_events.end() && epoch != _epochs.find(seqnr)->second) {
        throw std::runtime_error("A repromise cannot change the epoch.");
    }
#endif

    // Renewed promises must have <= previous num_events; otherwise ignore as stale/delayed.
    auto it_event = _num_events.find(seqnr);
    if (it_event != _num_events.end() && num_events > it_event->second) {
        return false;
    }

    _num_events[seqnr] = num_events;
    _epochs[seqnr] = epoch;

    auto it_count = _event_count.find(epoch);
    if (it_count == _event_count.end()) {
        _event_count[epoch] = 0;
    } else if (it_count->second > num_events) {
        throw std::runtime_error("More events received than promised.");
    }

    if (seqnr == _seqnr + 1UL) {
        _next_epoch = epoch;
        _tryNextEpoch();
        return true;
    }
    return false;
}

double PredecessorEventQueue::getEpoch() const {
    std::lock_guard<std::mutex> lock(_mtx);
    return _epoch;
}

double PredecessorEventQueue::getNextEpoch() const {
    std::lock_guard<std::mutex> lock(_mtx);
    return _next_epoch;
}

bool PredecessorEventQueue::empty() const {
    std::lock_guard<std::mutex> lock(_mtx);
    return _events.empty() || _events.top().epoch > _epoch;
}

bool PredecessorEventQueue::waitingForPromise() const {
    std::lock_guard<std::mutex> lock(_mtx);
    return _num_events.find(_seqnr + 1UL) == _num_events.end();
}

bool PredecessorEventQueue::waitingForEvents() const {
    // Note: these both lock internally, but are small; if you prefer, inline logic under one lock.
    return empty() && !waitingForPromise();
}

std::string PredecessorEventQueue::dump() const {
    std::lock_guard<std::mutex> lock(_mtx);

    std::string result;
    result.append("Epoch: " + std::to_string(_epoch) + "\n");
    result.append("Next epoch: " + std::to_string(_next_epoch) + "\n");

    result.append("Event counts:\n");
    for (const auto& kvp : _event_count) {
        result.append("  " + std::to_string(kvp.first) + ": " + std::to_string(kvp.second) + "\n");
    }

    result.append("Num events:\n");
    for (const auto& kvp : _num_events) {
        result.append("  " + std::to_string(kvp.first) + ": " + std::to_string(kvp.second) + "\n");
    }

    result.append("Epochs:\n");
    for (const auto& kvp : _epochs) {
        result.append("  " + std::to_string(kvp.first) + ": " + std::to_string(kvp.second) + "\n");
    }

    return result;
}

}  // namespace disco
