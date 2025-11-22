cmake_minimum_required(VERSION 3.20)
project(disco LANGUAGES CXX)

# Toggle: turn OFF to build pure-Python wheels (no compiled code)
option(BUILD_EXT "Build the C++/pybind11 extension" ON)

# If you don't build the extension, still allow skbuild to package Python sdist/wheel
if(NOT BUILD_EXT)
  message(STATUS "BUILD_EXT=OFF → building pure-Python wheel (no native extension)")
  return()
endif()

# --- Native build path (only if BUILD_EXT=ON) ------------------------
find_package(Python3 COMPONENTS Interpreter Development.Module REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/disco)
set(CPP_DIR ${SRC_DIR}/cpp)

pybind11_add_module(_core
  ${SRC_DIR}/_core.cpp
)

target_include_directories(_core PRIVATE ${CPP_DIR})

if(APPLE AND NOT DEFINED CMAKE_OSX_DEPLOYMENT_TARGET)
  set(CMAKE_OSX_DEPLOYMENT_TARGET "10.15" CACHE STRING "" FORCE)
endif()

