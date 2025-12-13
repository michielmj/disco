#define DEBUG_ENABLED

#ifndef DISCO_PREDECESSOREVENTQUEUE_H
#define DISCO_PREDECESSOREVENTQUEUE_H

#include <cmath>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <vector>

// Forward declare PyObject to avoid including Python.h in a public header.
struct _object;
using PyObject = _object;

namespace disco {

struct PredecessorEvent {
    double epoch{};
    PyObject* data{nullptr};
    std::map<std::string, std::string> headers{};

    PredecessorEvent() noexcept;

    // Construct from BORROWED PyObject* (will INCREF).
    PredecessorEvent(double epoch_,
                     PyObject* data_,
                     std::map<std::string, std::string> headers_);

    PredecessorEvent(const PredecessorEvent& other);
    PredecessorEvent& operator=(const PredecessorEvent& other);

    PredecessorEvent(PredecessorEvent&& other) noexcept;
    PredecessorEvent& operator=(PredecessorEvent&& other) noexcept;

    ~PredecessorEvent();

    // Transfer ownership of the PyObject* out of this event (prevents DECREF in destructor).
    PyObject* release_data() noexcept;

    bool operator<(const PredecessorEvent& other) const {
        // priority_queue puts "largest" on top; invert for min-heap semantics
        return epoch > other.epoch;
    }
};

class PredecessorEventQueue {
public:
    PredecessorEventQueue();

    bool push(double epoch, PyObject* data, std::map<std::string, std::string>& headers);

    std::vector<PredecessorEvent> pop();

    std::vector<PredecessorEvent> popAll();

    bool promise(unsigned long seqnr, double epoch, unsigned long num_events);

    bool tryNextEpoch();

    double getEpoch() const;

    double getNextEpoch() const;

    bool waitingForPromise() const;

    bool waitingForEvents() const;

    bool empty() const;

    std::string dump() const;

private:
    bool _tryNextEpoch();          // requires _mtx held
    bool _increment(double epoch); // requires _mtx held

    double epochAfter(unsigned long seqnr);   // requires _mtx held
    double epochBefore(unsigned long seqnr);  // requires _mtx held

private:
    mutable std::mutex _mtx;

    std::map<double, unsigned long> _event_count;
    std::map<unsigned long, unsigned long> _num_events;
    std::map<unsigned long, double> _epochs;

    std::priority_queue<PredecessorEvent, std::deque<PredecessorEvent>> _events;

    double _epoch;
    double _next_epoch;
    unsigned long _seqnr;
};

}  // namespace disco

#endif  // DISCO_PREDECESSOREVENTQUEUE_H
