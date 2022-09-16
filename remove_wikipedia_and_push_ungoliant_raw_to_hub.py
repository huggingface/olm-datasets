from datasets import Dataset, DatasetInfo
from tqdm import tqdm
import pandas as pd
import sys

dset_suffix = sys.argv[1]
num_proc = int(sys.argv[2])
huggingface_username = sys.argv[3]

# For some reason, datasets errors out if we try to load directly from the jsonl, so we need to do this first
i = 0
print("Chunking the ungoliant json into several parquet files before loading into huggingface dataset.")
for chunk in tqdm(pd.read_json("ungoliant_pipeline_results/en_meta.jsonl", lines=True, chunksize=100000)):
    chunk.to_parquet("ungoliant_pipeline_results_en_parquet/" + str(i) + ".parquet")
    i += 1

d = Dataset.from_parquet(path_or_paths=["ungoliant_pipeline_results_en_parquet/" + str(idx) + ".parquet" for idx in range(i)])

# Filter the dataset to remove wikipedia urls (we assume that the final dataset will also have a full dump of wikipedia, and we want to deduplicate it).
d = d.filter(lambda example: not example["warc_headers"]["warc-target-uri"].startswith("https://en.wikipedia.org/wiki/"), num_proc=num_proc)

# Reformat the dataset to work with the bigscience filtering and deduplication scripts.
d = d.map(lambda example, idx: {"id": idx, "text": example["content"], "meta": dict(**example["metadata"], **{"headers": example["warc_headers"]})}, num_proc=num_proc, with_indices=True, remove_columns=["content", "warc_headers", "metadata"])

d.push_to_hub("Tristan/ungoliant-for-olm-raw-" + sys.argv[1])
