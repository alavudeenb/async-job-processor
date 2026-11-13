def calculate_retry_delay(attempt_count: int) -> int:
    return min(5 * (2 ** (attempt_count - 1)), 300)
