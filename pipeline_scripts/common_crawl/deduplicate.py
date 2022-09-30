from datasets import load_dataset, load_from_disk
from text_dedup.exact_dedup import GoogleSuffixArrayDeduplicator
from shutil import rmtree
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input_dataset_name")
parser.add_argument("--output_dataset_name")
parser.add_argument("--text_column")
parser.add_argument("--url_column")
parser.add_argument("--timestamp_column")
parser.add_argument("--split", default=None)
parser.add_argument("--num_proc", type=int)
parser.add_argument("--push_to_hub", action="store_true")
parser.add_argument("--load_from_hub_instead_of_disk", action="store_true")
args = parser.parse_args()

if args.load_from_hub_instead_of_disk:
    if args.split is None:
        ds = load_dataset(args.input_dataset_name)
    else:
        ds = load_dataset(args.input_dataset_name, split=args.split)
else:
    if args.split is None:
        ds = load_from_disk(args.input_dataset_name)
    else:
        ds = load_from_disk(args.input_dataset_name)[args.split]

def check_for_ending_example_in_cluster(example, index, column, last_index):
    if index == last_index:
        return True
    return ds[index+1][column] != example[column]

# Sort the dataset so that examples with the same URL are grouped together, but also arrange by timestamp from oldest to newest.
print("Sorting by timestamp")
ds = ds.sort(args.timestamp_column)
print("Sorting by URL")
ds = ds.sort(args.url_column, kind="stable")

# Only keep one example per URL (the most recent example).
last_index = len(ds) - 1
ds = ds.filter(lambda example, index: check_for_ending_example_in_cluster(example, index, args.url_column, last_index), num_proc=args.num_proc, with_indices=True)

# Now sort the dataset so examples with the same text are grouped together.
print("Sorting by text")
ds = ds.sort(args.text_column, kind="stable")

# Filter away text that exactly matches other text.
last_index = len(ds) - 1
ds = ds.filter(lambda example, index: check_for_ending_example_in_cluster(example, index, args.text_column, last_index), num_proc=args.num_proc, with_indices=True)

# Now, do an aggressive form of Suffix Array Substring Exact Deduplication.
# If an example in our courpus has a byte string of 100 or longer which is duplicated elsewhere in the corpus, remove the whole example.
# In the paper for this deduplication method, they only remove the byte string, not the whole example. Removing the whole example will vastly
# shrink the size of the dataset, but it will ensure better quality data, without gaps in text continuity. We have plenty of data, so we opt for
# removing the whole example.
deduplicator = GoogleSuffixArrayDeduplicator(k=100)
slices = deduplicator.fit_predict(ds[args.text_column])
ds = ds.filter(lambda example, index: slices[index] == [], num_proc=args.num_proc, with_indices=True)

ds.save_to_disk(args.output_dataset_name)
rmtree(".cache")

if args.push_to_hub:
    ds.push_to_hub(args.output_dataset_name)
