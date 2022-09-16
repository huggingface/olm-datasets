from datasets import Dataset, DatasetInfo
import pandas as pd
import sys

dset_suffix = sys.argv[1]
num_proc = int(sys.argv[2])
huggingface_username = sys.argv[3]

d = Dataset.from_csv("outputs/en/text.csv")
d = d.map(lambda example, idx: {"id": idx}, num_proc=num_proc, with_indices=True)
d.push_to_hub(huggingface_username + "/ungoliant-for-olm-clean-" + dset_suffix)
