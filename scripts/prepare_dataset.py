import base64
from typing import List, Text, TypeVar

import numpy as np
import numpy.typing as npt
import pyarrow as pa
import pyarrow.parquet as pq
from datasets import load_dataset
from diskcache import FanoutCache
from FlagEmbedding import BGEM3FlagModel
from pydantic import BaseModel
from tqdm import tqdm

T = TypeVar("T")

dataset_name = "Helsinki-NLP/opus-100"
subsets = [
    "af-en",
    "ar-en",
    "bn-en",
    "de-en",
    "en-es",
    "en-fr",
    "en-hi",
    "en-id",
    "en-it",
    "en-ja",
    "en-pt",
    "en-ru",
    "en-tr",
    "en-ur",
    "en-vi",
    "en-zh",
]
splits = ["train", "validation", "test"]
langs = [
    "ar",
    "bn",
    "de",
    "en",
    "es",
    "fr",
    "hi",
    "id",
    "it",
    "ja",
    "pt",
    "ru",
    "tr",
    "ur",
    "vi",
    "zh",
]
dataset_intermediate_path = "data/opus-100-intermediate.parquet"

compression = "snappy"
cache = FanoutCache("data/BAAI__bge_m3.cache")
model = BGEM3FlagModel("BAAI/bge-m3")


def np_to_base64(array: npt.NDArray) -> Text:
    return base64.b64encode(array.tobytes()).decode("utf-8")


def base64_to_np(base64_str: Text, dtype: npt.DTypeLike = np.float32) -> npt.NDArray:
    return np.frombuffer(base64.b64decode(base64_str), dtype=dtype)


def embedding_texts(
    texts: List[Text], embedding_size: int = 1024, dtype: npt.DTypeLike = np.float32
) -> npt.NDArray[np.float32]:
    embeddings = np.empty((len(texts), embedding_size), dtype=dtype)
    cached_indices = []
    no_cached_indices = []

    for i, text in enumerate(texts):
        if text in cache:
            print(f"cache hit: {text}")
            embeddings[i] = base64_to_np(cache[text])  # type: ignore
            cached_indices.append(i)
        else:
            no_cached_indices.append(i)

    if cached_indices and len(cached_indices) == len(texts):
        return embeddings
    else:
        no_cached_texts = [texts[i] for i in no_cached_indices]
        no_cached_embeddings = model.encode(no_cached_texts)["dense_vecs"]
        for _text, (i, j) in zip(no_cached_texts, enumerate(no_cached_indices)):
            cache[_text] = np_to_base64(no_cached_embeddings[i])
            embeddings[j] = no_cached_embeddings[i]

    return embeddings


def process_dataset_intermediate(save_dataset_path: Text = dataset_intermediate_path):
    id_accumulator = 0

    def _process_dataset_intermediate_in_chunks(
        dataset, subset: Text, split: Text, chunk_size: int = 10000
    ):
        nonlocal id_accumulator
        batch = []
        lang_en = "en"
        lang_tar = subset.replace(lang_en, "").strip(" _-")
        for row in tqdm(dataset, leave=False, desc=f"{subset}:{split}"):
            trans_data = row["translation"]
            batch.extend(
                [
                    PointIntermediate.model_validate(
                        {
                            "id": id_accumulator + 1,
                            "text": trans_data[lang_en],
                            "language": lang_en,
                            "split": split,
                            "subset": subset,
                            "source": dataset_name,
                        }
                    ).model_dump(),
                    PointIntermediate.model_validate(
                        {
                            "id": id_accumulator + 2,
                            "text": trans_data[lang_tar],
                            "language": lang_tar,
                            "split": split,
                            "subset": subset,
                            "source": dataset_name,
                        }
                    ).model_dump(),
                ]
            )
            id_accumulator += 2

            if len(batch) >= chunk_size:
                yield pa.Table.from_pylist(batch)
                batch = []

        if batch:
            yield pa.Table.from_pylist(batch)

    parquet_writer = None
    # for subset in subsets:
    #     for split in splits:
    subsets = ["en-vi", "en-zh"]
    splits = ["test"]
    for subset in tqdm(subsets, leave=False, desc="Processing subsets"):
        for split in tqdm(splits, leave=False, desc="Processing splits"):
            dataset = load_dataset(dataset_name, subset, split=split)
            for table in _process_dataset_intermediate_in_chunks(
                dataset, subset=subset, split=split
            ):
                if parquet_writer is None:
                    parquet_writer = pq.ParquetWriter(
                        save_dataset_path, table.schema, compression=compression
                    )
                parquet_writer.write_table(table)

    if parquet_writer:
        parquet_writer.close()

    print(f"Dataset successfully saved as '{save_dataset_path}'")


class PointIntermediate(BaseModel):
    id: int
    text: Text
    language: Text
    split: Text
    subset: Text
    source: Text


class PointFeature(PointIntermediate):
    model: Text
    embedding_base64: Text


def main():
    process_dataset_intermediate()


if __name__ == "__main__":
    main()
