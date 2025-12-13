#ifndef DISCO_EVENT_QUEUE_H
#define DISCO_EVENT_QUEUE_H

#include "PredecessorEventQueue.h"

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

// Forward declare PyObject to avoid including Python.h in public headers.
struct _object;
using PyObject = _object;

namespace disco {

struct BorrowedRefTag {};
struct OwnedRefTag {};
inline constexpr BorrowedRefTag borrowed_ref{};
inline constexpr OwnedRefTag owned_ref{};

struct Event {
    std::string sender;
    double epoch;
    PyObject* data;
    std::map<std::string, std::string> headers;

    Event() noexcept;

    // Construct from a BORROWED PyObject* (will INCREF).
    Event(std::string sender_,
          double epoch_,
          PyObject* data_,
          std::map<std::string, std::string> headers_,
          BorrowedRefTag);

    // Construct from an OWNED PyObject* (no INCREF).
    Event(std::string sender_,
          double epoch_,
          PyObject* data_,
          std::map<std::string, std::string> headers_,
          OwnedRefTag) noexcept;

    Event(const Event& other);
    Event& operator=(const Event& other);

    Event(Event&& other) noexcept;
    Event& operator=(Event&& other) noexcept;

    ~Event();

    // Transfer ownership of the PyObject* out of this Event (prevents DECREF in destructor).
    PyObject* release_data() noexcept;
};

class EventQueue {
public:
    EventQueue();
    ~EventQueue() = default;

    double getEpoch() const;
    double getNextEpoch() const;
    const std::string& getWaitingFor() const;

    bool push(const std::string& sender,
              double epoch,
              PyObject* data,
              std::map<std::string, std::string>& headers);

    std::vector<Event> pop();
    std::vector<Event> popAll();

    bool promise(const std::string& sender,
                 unsigned long seqnr,
                 double epoch,
                 unsigned long num_events);

    bool tryNextEpoch();

    bool hasPredecessors() const;
    void registerPredecessor(const std::string& predecessor);

    bool empty() const;

private:
    struct PredSnapshot {
        std::string name;
        double epoch;
        double next_epoch;
        bool empty;
    };

    bool tryNextEpochUnlocked(); // requires _mtx held
    std::vector<PredSnapshot> snapshotPredecessorsUnlocked() const; // requires _mtx held

private:
    mutable std::mutex _mtx;

    std::map<std::string, std::unique_ptr<PredecessorEventQueue>> _predecessors;

    double _epoch;
    double _next_epoch;
    std::string _waiting_for;
};

}  // namespace disco

#endif  // DISCO_EVENT_QUEUE_H
