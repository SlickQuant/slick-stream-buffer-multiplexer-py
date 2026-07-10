"""
Cleanup utility for shared memory segments from test runs.

Run this if tests fail to clean up properly and you get "File exists" errors.
"""
from multiprocessing import shared_memory
import sys

# Known test shared memory segment names
TEST_SHM_NAMES = [
    "mux_py_rt_records", "mux_py_rt_p0", "mux_py_rt_p1",
    "mux_py_part_records", "mux_py_part_p0",
    "mux_py_dup_records", "mux_py_dup_p0",
    "mux_py_geo_records", "mux_py_geo_records2", "mux_py_geo_p0",
    "mux_py_noprod_records",
    "mux_py_xp_records", "mux_py_xp_p0", "mux_py_xp_p1",
    "mux_io_p2c_records", "mux_io_p2c_p0", "mux_io_p2c_p1",
    "mux_io_c2p_records", "mux_io_c2p_p0", "mux_io_c2p_p1",
    "mux_io_mix_records", "mux_io_mix_p0", "mux_io_mix_p1",
    "mux_io_part_records", "mux_io_part_p0", "mux_io_part_p1",
]


def cleanup_shm():
    """Attempt to unlink all test shared memory segments."""
    cleaned = []
    failed = []

    for name in TEST_SHM_NAMES:
        try:
            shm = shared_memory.SharedMemory(name=name)
            try:
                shm.close()
            except BufferError:
                pass
            shm.unlink()
            cleaned.append(name)
            print(f"Cleaned up: {name}")
        except FileNotFoundError:
            pass
        except Exception as e:
            failed.append((name, str(e)))
            print(f"Failed to clean {name}: {e}")

    print(f"\n{len(cleaned)} segment(s) cleaned, {len(failed)} failed")
    return len(failed) == 0


if __name__ == '__main__':
    sys.exit(0 if cleanup_shm() else 1)
