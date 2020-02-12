from functools import lru_cache
from pathlib import Path
from typing import Tuple, Union, overload

BUFFER_BITS = 14
BUFFER_LEN = 1 << BUFFER_BITS  # 16k
BUFFER_MASK = BUFFER_LEN - 1


class FileBytes:
    def __init__(self, path: Path, max_cache: int = 4):
        self.path = path
        self._len = self.path.stat().st_size

        last_position = 0
        fp = path.open("rb")

        @lru_cache(maxsize=max_cache)
        def load_segment(seg: int) -> bytes:
            nonlocal last_position
            position = seg << BUFFER_BITS
            if last_position != position:
                fp.seek(position)
            buffer = fp.read(BUFFER_LEN)
            last_position = position + len(buffer)
            return buffer

        self.load_segment = load_segment

    def __len__(self):
        return self._len

    def seg_split(self, position: int) -> Tuple[int, int]:
        """
        :return:
            segment - segment index
            offset - offset within segment
        """
        return position >> BUFFER_BITS, position & BUFFER_MASK

    @overload
    def __getitem__(self, item: int) -> int:
        ...

    @overload
    def __getitem__(slice, item: slice) -> bytes:
        ...

    def __getitem__(self, item) -> Union[int, bytes]:
        if isinstance(item, int):
            if item < 0:
                item += self._len
            if item < 0 or item >= self._len:
                raise IndexError(f"index out of range {item}")
            seg, idx = self.seg_split(item)
            return self.load_segment(seg)[idx]
        elif isinstance(item, slice):
            start, stop, step = item.indices(len(self))
            assert step == 1

            def load():
                start_seg, start_idx = self.seg_split(start)
                end_seg, end_idx = self.seg_split(stop)
                if start_seg == end_seg:
                    yield self.load_segment(start_seg)[start_idx:end_idx]
                else:
                    yield self.load_segment(start_seg)[start_idx:]
                    for seg in range(start_seg + 1, end_seg):
                        yield self.load_segment(seg)
                    yield self.load_segment(end_seg)[:end_idx]

            return b"".join(load())
        raise KeyError(f"Not sure what to do with {item}")
