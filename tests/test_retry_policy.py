import pytest

from app.retry_policy import calculate_retry_delay


@pytest.mark.parametrize(
    "attempt_count, expected_delay",
    [
        (1, 5),
        (2, 10),
        (3, 20),
        (4, 40),
        (5, 80),
        (6, 160),
        (7, 300),
        (8, 300),
        (10, 300),
    ],
)
def test_calculate_retry_delay(attempt_count, expected_delay):
    assert calculate_retry_delay(attempt_count) == expected_delay
