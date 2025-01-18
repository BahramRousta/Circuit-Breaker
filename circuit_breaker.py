import logging
import threading
import time
from typing import Callable
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


class CircuitBreaker:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if not cls._instance:
                cls._instance = super().__new__(cls)
                cls._instance.__initialized = False

        return cls._instance

    def __init__(
        self, max_failures: int, reset_timeout: int, half_open_max_requests: int,
    ):
        if self.__initialized:
            return

        self.max_failures = max_failures
        self.reset_timeout = reset_timeout
        self.half_open_max_requests = half_open_max_requests
        self.failure_count = 0
        self.last_failure_time = None

        self.state = "CLOSED"
        self.half_open_request_count = 0

        self.__initialized = True

    def call(self, func: Callable, *args, **kwargs):
        with self._lock:
            if self.state == "OPEN":
                if (time.time() - self.last_failure_time) >= self.reset_timeout:
                    logger.info("Reset timeout elapsed. Transitioning to HALF-OPEN.")
                    self.state = "HALF-OPEN"
                    self.half_open_request_count = 0
                else:
                    logger.warning("Circuit is OPEN. Service call blocked.")
                    raise RuntimeError("Circuit is open. Service call blocked.")

        if self.state == "HALF-OPEN":
            if self.half_open_request_count >= self.half_open_max_requests:
                logger.warning("Circuit is HALF-OPEN. Too many test requests.")
                raise RuntimeError("Circuit is HALF-OPEN. Too many test requests.")
            self.half_open_request_count += 1

        try:
            result = func(*args, **kwargs)
            with self._lock:
                self.reset()
            return result
        except Exception as e:
            with self._lock:
                self.record_failure()
            logger.error(f"Function call failed: {e}")
            raise

    def record_failure(self):
        self.failure_count += 1
        if self.state == "HALF-OPEN":
            logger.info("HALF-OPEN request failed. Transitioning to OPEN.")
            self.state = "OPEN"
        elif self.failure_count >= self.max_failures:
            logger.info("Max failures reached. Transitioning to OPEN.")
            self.state = "OPEN"

        self.last_failure_time = time.time()

    def reset(self):
        logger.info("Resetting circuit breaker to CLOSED state.")
        self.failure_count = 0
        self.state = "CLOSED"
        self.half_open_request_count = 0


def unreliable_service(should_fail=False):
    if should_fail:
        raise ValueError("Service failed!")
    return "Service succeeded!"


circuit_breaker = CircuitBreaker(max_failures=3, reset_timeout=3, half_open_max_requests=2)


def make_request(should_fail=False):
    try:
        result = circuit_breaker.call(unreliable_service, should_fail=should_fail)
        print(result)
    except RuntimeError as e:
        print(f"RuntimeError: {e}")
    except Exception as e:
        print(f"Service call exception: {e}")


print("\n--- Initial successful requests ---")
make_request()  # Service succeeds
make_request()  # Service succeeds


print("\n--- Simulating failures ---")
make_request(should_fail=True)  # Service fails
make_request(should_fail=True)  # Service fails
make_request(should_fail=True)  # Service fails
make_request(should_fail=True)  # Service fails (reaches max failures, circuit opens)

print("\n--- Waiting for reset timeout ---")
time.sleep(3)  # Wait for reset_timeout to elapse


print("\n--- Circuit transitions to HALF-OPEN ---")
make_request()  # Service succeeds, circuit remains HALF-OPEN
make_request(should_fail=True)  # Service succeeds, circuit closes
make_request(should_fail=True)
make_request()

print("\n--- Back to normal operation ---")
make_request()