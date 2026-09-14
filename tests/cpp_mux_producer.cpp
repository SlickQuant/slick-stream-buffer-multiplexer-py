//
// C++ producer for the slick-stream-buffer-multiplexer-py interop tests.
//
// Usage:
//   cpp_mux_producer <queue_name> <producer_shm_name> <producer_id> <num_messages>
//                    [create-queue <queue_size>]
//                    [create-producer <capacity> <control_size>]
//                    [--hold] [--delay-us N] [--no-read-last]
//
//   create-queue     create the shared record queue; otherwise open existing
//   create-producer  create the producer segment; otherwise open existing
//   --hold           after publishing, wait for a line on stdin before exiting
//                    (keeps the segments alive on Windows so Python can attach)
//   --delay-us N     sleep N microseconds between messages
//   --no-read-last   use interop_no_read_last_traits instead of the default
//                    traits: a created queue is marked 'SLQ0', and an opened
//                    one must already be, or construction throws
//
// Prints "READY" once the queue and producer are constructed, then
// "PUBLISHED <n>" on success. Exits 0.
//
#include <slick/stream_buffer_multiplexer.hpp>
#include "interop_common.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

namespace {

struct options {
    const char* queue_name = nullptr;
    const char* producer_shm_name = nullptr;
    uint32_t producer_id = 0;
    int num_messages = 0;
    bool create_queue = false;
    uint32_t queue_size = 0;
    bool create_producer = false;
    uint64_t capacity = 0;
    uint32_t control_size = 0;
    bool hold = false;
    int delay_us = 0;
};

template<typename Traits>
int produce(const options& opt) {
    using multiplexer = slick::basic_stream_buffer_multiplexer<Traits>;
    try {
        std::unique_ptr<multiplexer> mux;
        if (opt.create_queue) {
            mux = std::make_unique<multiplexer>(opt.queue_size, opt.queue_name);
        } else {
            mux = std::make_unique<multiplexer>(opt.queue_name);
        }

        std::shared_ptr<typename multiplexer::producer_buffer> pb;
        if (opt.create_producer) {
            pb = mux->add_producer(opt.producer_id, opt.capacity, opt.control_size,
                                   opt.producer_shm_name);
        } else {
            pb = mux->add_producer(opt.producer_id, opt.producer_shm_name);
        }

        std::printf("READY\n");
        std::fflush(stdout);

        for (int i = 0; i < opt.num_messages; ++i) {
            const std::size_t len = interop_msg_length(static_cast<uint32_t>(i));
            auto [ptr, sz] = pb->prepare(len);
            if (ptr == nullptr || sz != len) {
                std::fprintf(stderr, "prepare(%zu) failed at message %d\n", len, i);
                return 1;
            }
            interop_write_msg(ptr, opt.producer_id, static_cast<uint32_t>(i));
            pb->commit(len);
            pb->consume(len);
            if (opt.delay_us > 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(opt.delay_us));
            }
        }

        std::printf("PUBLISHED %d\n", opt.num_messages);
        std::fflush(stdout);

        if (opt.hold) {
            std::string line;
            std::getline(std::cin, line);
        }
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "cpp_mux_producer error: %s\n", e.what());
        return 1;
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 5) {
        std::fprintf(stderr,
            "Usage: %s <queue_name> <producer_shm_name> <producer_id> <num_messages> "
            "[create-queue <queue_size>] [create-producer <capacity> <control_size>] "
            "[--hold] [--delay-us N] [--no-read-last]\n", argv[0]);
        return 2;
    }

    options opt;
    opt.queue_name = argv[1];
    opt.producer_shm_name = argv[2];
    opt.producer_id = static_cast<uint32_t>(std::atoi(argv[3]));
    opt.num_messages = std::atoi(argv[4]);
    bool no_read_last = false;

    for (int i = 5; i < argc; ++i) {
        if (std::strcmp(argv[i], "create-queue") == 0 && i + 1 < argc) {
            opt.create_queue = true;
            opt.queue_size = static_cast<uint32_t>(std::strtoul(argv[i + 1], nullptr, 10));
            ++i;
        } else if (std::strcmp(argv[i], "create-producer") == 0 && i + 2 < argc) {
            opt.create_producer = true;
            opt.capacity = std::strtoull(argv[i + 1], nullptr, 10);
            opt.control_size = static_cast<uint32_t>(std::strtoul(argv[i + 2], nullptr, 10));
            i += 2;
        } else if (std::strcmp(argv[i], "--hold") == 0) {
            opt.hold = true;
        } else if (std::strcmp(argv[i], "--delay-us") == 0 && i + 1 < argc) {
            opt.delay_us = std::atoi(argv[i + 1]);
            ++i;
        } else if (std::strcmp(argv[i], "--no-read-last") == 0) {
            no_read_last = true;
        } else {
            std::fprintf(stderr, "Unknown argument: %s\n", argv[i]);
            return 2;
        }
    }

    return no_read_last ? produce<interop_no_read_last_traits>(opt)
                        : produce<slick::default_queue_traits>(opt);
}
