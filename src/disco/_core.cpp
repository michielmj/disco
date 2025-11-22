#include <pybind11/pybind11.h>

namespace py = pybind11;

PYBIND11_MODULE(_core, m) {
  m.doc() = "Core C++ extension for disco";
  m.def("example", []() { return 42; }, "Example function");
}

