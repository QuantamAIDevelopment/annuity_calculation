from typing import Iterable


def chunked(iterable: Iterable, size: int):
    """Yield successive chunks of given size from iterable."""
    it = iter(iterable)
    while True:
        chunk = []
        try:
            for _ in range(size):
                chunk.append(next(it))
        except StopIteration:
            if chunk:
                yield chunk
            break
        yield chunk
