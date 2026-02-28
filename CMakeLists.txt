cmake_minimum_required(VERSION 3.10)
project(Ordering)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
include(FetchContent)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)


# Debug target
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_definitions(DEBUG)
endif()



add_executable(${PROJECT_NAME} main.cpp)
# target_link_libraries(${PROJECT_NAME} PRIVATE)
