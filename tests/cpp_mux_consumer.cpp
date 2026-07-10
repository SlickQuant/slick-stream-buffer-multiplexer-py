//
// C++ consumer for the slick-stream-buffer-multiplexer-py interop tests.
//
// Usage:
//   cpp_mux_consumer <queue_name> <num_messages>
//                    --producer <id> <shm_name> [--producer <id> <shm_name> ...]
//                    [--timeout-s N]
//
// Opens the shared record queue and the listed producer segments, then reads
// num_messages messages in global order. Every message's framing must be
// valid (worker_id == producer_id) and every producer's msg_num sequence must
// be strictly sequential from 0. Records referencing unregistered producer
// ids are skipped by the multiplexer itself.
//
// Prints "CONSUMED <n>" on success and exits 0.
//
#include <slick/stream_buffer_multiplexer.hpp>
#include "interop_common.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <thread>

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr,
            "Usage: %s <queue_name> <num_messages> --producer <id> <shm_name> "
            "[--producer <id> <shm_name> ...] [--timeout-s N]\n", argv[0]);
        return 2;
    }

    const char* queue_name = argv[1];
    const int num_messages = std::atoi(argv[2]);
    int timeout_s = 30;

    std::map<uint32_t, const char*> producer_names;
    for (int i = 3; i < argc; ++i) {
        if (std::strcmp(argv[i], "--producer") == 0 && i + 2 < argc) {
            producer_names[static_cast<uint32_t>(std::atoi(argv[i + 1]))] = argv[i + 2];
            i += 2;
        } else if (std::strcmp(argv[i], "--timeout-s") == 0 && i + 1 < argc) {
            timeout_s = std::atoi(argv[i + 1]);
            ++i;
        } else {
            std::fprintf(stderr, "Unknown argument: %s\n", argv[i]);
            return 2;
        }
    }

    try {
        slick::stream_buffer_multiplexer mux(queue_name);
        for (const auto& [id, name] : producer_names) {
            mux.add_producer(id, name);
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
        std::map<uint32_t, uint32_t> expected;  // per-producer next msg_num

        uint64_t cursor = 0;
        int received = 0;
        while (received < num_messages) {
            if (std::chrono::steady_clock::now() >= deadline) {
                std::fprintf(stderr, "timed out after %d of %d messages\n", received, num_messages);
                return 1;
            }
            auto rec = mux.read(cursor);
            if (!rec) {
                std::this_thread::yield();
                continue;
            }

            uint32_t msg_num = 0;
            if (!interop_check_msg(rec.data, rec.length, rec.producer_id, &msg_num)) {
                std::fprintf(stderr, "corrupt message from producer %u after %d received (len=%u)\n",
                             rec.producer_id, received, rec.length);
                return 1;
            }
            uint32_t& next = expected[rec.producer_id];
            if (msg_num != next) {
                std::fprintf(stderr, "producer %u: expected msg_num %u, got %u\n",
                             rec.producer_id, next, msg_num);
                return 1;
            }
            ++next;
            ++received;
        }

        std::printf("CONSUMED %d\n", received);
        for (const auto& [id, count] : expected) {
            std::printf("PRODUCER %u %u\n", id, count);
        }
        std::fflush(stdout);
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "cpp_mux_consumer error: %s\n", e.what());
        return 1;
    }
}
