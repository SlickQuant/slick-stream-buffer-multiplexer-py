include(FetchContent)

# Fetch slick-stream-buffer-multiplexer from GitHub (its own CMake pulls
# slick-stream-buffer and slick-queue, which pull slick-shm).
#
# For local development without network access, point the dependencies at
# local checkouts when configuring:
#   -DFETCHCONTENT_SOURCE_DIR_SLICK-STREAM-BUFFER-MULTIPLEXER=Y:/repo/slick-stream-buffer-multiplexer
#   -DFETCHCONTENT_SOURCE_DIR_SLICK-STREAM-BUFFER=Y:/repo/slick-stream-buffer
#   -DFETCHCONTENT_SOURCE_DIR_SLICK-QUEUE=Y:/repo/slick-queue
#   -DFETCHCONTENT_SOURCE_DIR_SLICK-SHM=Y:/repo/slick-shm
set(BUILD_SLICK_STREAM_BUFFER_MULTIPLEXER_TESTS OFF CACHE BOOL "" FORCE)
set(BUILD_SLICK_STREAM_BUFFER_TESTS OFF CACHE BOOL "" FORCE)
set(BUILD_SLICK_QUEUE_TESTS OFF CACHE BOOL "" FORCE)
set(SLICK_SHM_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(SLICK_SHM_BUILD_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    slick-stream-buffer-multiplexer
    GIT_REPOSITORY https://github.com/SlickQuant/slick-stream-buffer-multiplexer.git
    GIT_TAG v2.2.0
)
FetchContent_MakeAvailable(slick-stream-buffer-multiplexer)

# Find Python
find_package(Python3 COMPONENTS Interpreter REQUIRED)

# Interop test executables built against the REAL C++ header
add_executable(cpp_mux_producer cpp_mux_producer.cpp)
target_link_libraries(cpp_mux_producer PRIVATE slick::stream_buffer_multiplexer)

add_executable(cpp_mux_consumer cpp_mux_consumer.cpp)
target_link_libraries(cpp_mux_consumer PRIVATE slick::stream_buffer_multiplexer)

if(UNIX AND NOT APPLE)
    target_link_libraries(cpp_mux_producer PRIVATE pthread rt)
    target_link_libraries(cpp_mux_consumer PRIVATE pthread rt)
elseif(APPLE)
    target_link_libraries(cpp_mux_producer PRIVATE pthread)
    target_link_libraries(cpp_mux_consumer PRIVATE pthread)
endif()

# Pure-Python test suites
add_test(
    NAME multiplexer_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_multiplexer.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME multiplexer_mpmc_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_multiplexer_mpmc.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME multiplexer_shm_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_multiplexer_shm.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME multiplexer_queue_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_multiplexer_queue.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

# Python/C++ interop tests
add_test(
    NAME python_producers_cpp_consumer
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_interop.py --test python_producers_cpp_consumer
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME cpp_producers_python_consumer
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_interop.py --test cpp_producers_python_consumer
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME mixed_language_producers
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_interop.py --test mixed_language_producers
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME partial_registration_interop
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_interop.py --test partial_registration_interop
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

# enable_read_last=False ('SLQ0') must match across languages, whichever creates the queue
add_test(
    NAME read_last_off_python_queue
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_interop.py --test read_last_off_python_queue
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME read_last_off_cpp_queue
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_interop.py --test read_last_off_cpp_queue
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

# Queue producers (slick::queue<uint8_t>) across languages, in both directions
add_test(
    NAME python_queue_producer_cpp_consumer
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_interop.py --test python_queue_producer_cpp_consumer
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME cpp_queue_producer_python_consumer
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_interop.py --test cpp_queue_producer_python_consumer
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

set_tests_properties(
    multiplexer_test
    multiplexer_mpmc_test
    multiplexer_shm_test
    multiplexer_queue_test
    python_queue_producer_cpp_consumer
    cpp_queue_producer_python_consumer
    python_producers_cpp_consumer
    cpp_producers_python_consumer
    mixed_language_producers
    partial_registration_interop
    read_last_off_python_queue
    read_last_off_cpp_queue
    PROPERTIES TIMEOUT 120
)
