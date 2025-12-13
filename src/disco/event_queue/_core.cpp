#include <cmath>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "EventQueue.h"

namespace py = pybind11;

namespace {

py::list events_to_py_list(std::vector<disco::Event>&& events) {
    py::list out;
    for (auto& ev : events) {
        // Transfer ownership of the owned PyObject* reference from C++ to Python.
        py::object data = py::reinterpret_steal<py::object>(ev.release_data());
        py::dict headers = py::cast(std::move(ev.headers));
        out.append(py::make_tuple(std::move(ev.sender), ev.epoch, std::move(data), std::move(headers)));
    }
    return out;
}

}  // namespace

PYBIND11_MODULE(_core, m) {
    m.doc() = "Disco event queue (pybind11 binding)";

    py::class_<disco::EventQueue>(m, "EventQueue")
        .def(py::init<>())

        .def_property_readonly(
            "epoch",
            [](disco::EventQueue& q) -> double {
                // NOTE: We intentionally do NOT release the GIL here,
                // because the core implementation may INCREF/DECREF under internal locks.
                return q.getEpoch();
            })

        .def_property_readonly(
            "next_epoch",
            [](disco::EventQueue& q) -> py::object {
                const double v = q.getNextEpoch();
                if (std::isnan(v)) {
                    return py::none();
                }
                return py::float_(v);
            })

        .def_property_readonly(
            "waiting_for",
            [](disco::EventQueue& q) -> std::string {
                return q.getWaitingFor();
            })

        .def(
            "push",
            [](disco::EventQueue& q,
               const std::string& sender,
               double epoch,
               py::object data,
               py::object headers_obj) -> bool {
                std::map<std::string, std::string> headers;
                if (!headers_obj.is_none()) {
                    headers = headers_obj.cast<std::map<std::string, std::string>>();
                }

                // Core queue will INCREF the borrowed PyObject* internally (RAII).
                return q.push(sender, epoch, data.ptr(), headers);
            },
            py::arg("sender"),
            py::arg("epoch"),
            py::arg("data"),
            py::arg("headers") = py::none())

        .def(
            "pop",
            [](disco::EventQueue& q) -> py::list {
                return events_to_py_list(q.pop());
            })

        .def(
            "pop_all",
            [](disco::EventQueue& q) -> py::list {
                return events_to_py_list(q.popAll());
            })

        .def(
            "promise",
            [](disco::EventQueue& q,
               const std::string& sender,
               unsigned long seqnr,
               double epoch,
               unsigned long num_events) -> bool {
                return q.promise(sender, seqnr, epoch, num_events);
            },
            py::arg("sender"),
            py::arg("seqnr"),
            py::arg("epoch"),
            py::arg("num_events"))

        .def(
            "try_next_epoch",
            [](disco::EventQueue& q) -> bool {
                return q.tryNextEpoch();
            })

        .def_property_readonly(
            "has_predecessors",
            [](disco::EventQueue& q) -> bool {
                return q.hasPredecessors();
            })

        .def(
            "register_predecessor",
            [](disco::EventQueue& q, const std::string& predecessor) {
                q.registerPredecessor(predecessor);
            },
            py::arg("predecessor"))

        .def_property_readonly(
            "empty",
            [](disco::EventQueue& q) -> bool {
                return q.empty();
            });
}
