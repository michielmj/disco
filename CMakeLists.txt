cmake_minimum_required(VERSION 3.18)
project(disco LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Use modern FindPython. scikit-build-core sets Python_ROOT_DIR / Python_EXECUTABLE.
find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)
find_package(pybind11 CONFIG REQUIRED)

# ---- disco.event_queue._core ----

set(DISCO_EVENT_QUEUE_SRC
  src/disco/event_queue/_core.cpp
  src/disco/event_queue/EventQueue.cpp
  src/disco/event_queue/PredecessorEventQueue.cpp
)

pybind11_add_module(_core MODULE ${DISCO_EVENT_QUEUE_SRC})

target_include_directories(_core PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src/disco/event_queue
)

# On macOS/Linux, pybind11_add_module already links the right Python bits.
# If you find you need explicit linkage in some environments, prefer this:
# target_link_libraries(_core PRIVATE Python::Module)

# Install the extension into the Python package directory.
install(TARGETS _core
  LIBRARY DESTINATION disco/event_queue
  RUNTIME DESTINATION disco/event_queue
  ARCHIVE DESTINATION disco/event_queue
)

# Install the stub alongside the extension.
install(FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/src/disco/event_queue/_core.pyi
  DESTINATION disco/event_queue
)

# Mark the package as typed (PEP 561)
install(FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/src/disco/event_queue/py.typed
  DESTINATION disco/event_queue
)
