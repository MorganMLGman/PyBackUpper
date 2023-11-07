from datetime import datetime
from time import perf_counter

def size_to_human_readable(size: int) -> str:
    """Converts the size in bytes to a human readable format.

    Args:
        size (int): Size in bytes.

    Returns:
        str: Human readable size.
    """
    if isinstance(size, str):
        try:
            size = int(size)
        except ValueError:
            return f"{size}B"
    power = 2 ** 10
    n = 0
    power_labels = {0: '', 1: 'k', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    return f"{size:.2f}{power_labels[n]}B"

def timestamp_to_human_readable(timestamp: int) -> str:
    """Converts the timestamp to a human readable format.

    Args:
        timestamp (int): Timestamp.

    Returns:
        str: Human readable timestamp.
    """
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

def timestamp_to_file_name(timestamp: int) -> str:
    """Converts the timestamp to a file name.

    Args:
        timestamp (int): Timestamp.

    Returns:
        str: File name.
    """
    return datetime.fromtimestamp(timestamp).strftime("%Y_%m_%d_%H_%M_%S")


def timeit(func):
    """Decorator to measure the execution time of a function.

    Args:
        func (function): Function to measure.

    Returns:
        function: Decorated function.
    """
    def wrapper(*args, **kwargs):
        start = perf_counter()
        result = func(*args, **kwargs)
        end = perf_counter()
        print(f"Time elapsed: {end - start:.2f}s")
        return result
    return wrapper