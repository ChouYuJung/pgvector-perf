import base64
import itertools
from datetime import datetime
from typing import Generator, List, Literal, Sequence, Text, TypeVar, cast, overload

import numpy as np
import pytz

T = TypeVar("T")


def gen_session_id(prefix: Text = "pgv"):
    return f'pgv{datetime.now(pytz.utc).strftime("%Y%m%d%H%M%S")}'


def np_to_base64(arr: np.ndarray) -> bytes:
    return base64.b64encode(arr.tobytes())


def base64_to_np(base64_str: Text | bytes, dtype: Text = "float32") -> np.ndarray:
    base64_str = (
        base64_str.encode("utf-8") if isinstance(base64_str, Text) else base64_str
    )
    arr_bytes = base64.b64decode(base64_str)
    return np.frombuffer(arr_bytes, dtype=dtype)


@overload
def dummy_embedding(
    *shape: int, encoding_format: Literal["float"] = "float"
) -> List[float]: ...


@overload
def dummy_embedding(
    *shape: int, encoding_format: Literal["base64"] = "base64"
) -> bytes: ...


def dummy_embedding(
    *shape: int, encoding_format: Literal["float", "base64"] = "float"
) -> List[float] | bytes:
    array = np.random.rand(*shape).astype(np.float32)  # type: ignore
    array = cast(np.ndarray, array)
    return np_to_base64(array) if encoding_format == "base64" else array.tolist()


def batch_process(
    items: Sequence[T], batch_size: int = 32
) -> Generator[Sequence[T], None, None]:
    it = iter(items)
    chunk = tuple(itertools.islice(it, batch_size))
    while chunk:
        yield chunk
        chunk = tuple(itertools.islice(it, batch_size))
