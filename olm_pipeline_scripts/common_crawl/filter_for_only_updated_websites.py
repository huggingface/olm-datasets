from datasets import load_dataset
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input_dataset_name")
parser.add_argument("--output_dataset_name")
parser.add_argument("--text_column")
parser.add_argument("--timestamp_column")
parser.add_argument("--split")
parser.add_argument("--url_column")
parser.add_argument("--num_proc", type=int)
args = parser.parse_args()

ds = load_dataset(args.input_dataset_name, split=args.split)

# Group so examples with the same URL are next to each other in the dataset.
ds = ds.sort(args.url_column)

# Throw away examples with URLs occuring only once in the dataset.
last_index = len(ds) - 1
def check_for_adjacent_duplicate_url(example, index):
    if index == last_index:
        return ds[index-1][args.url_column] == example[args.url_column]
    if index == 0:
        return ds[index+1][args.url_column] == example[args.url_column]
    return ds[index-1][args.url_column] == example[args.url_column] or ds[index+1][args.url_column] == example[args.url_column]

ds = ds.filter(lambda example, index: check_for_adjacent_duplicate_url(example, index), num_proc=args.num_proc, with_indices=True)

# Sort the dataset so that examples with the same URL are still grouped together, but also arrange by timestamp from oldest to newest.
ds = ds.sort(args.timestamp_column)
ds = ds.sort(args.url_column, kind="stable")

# Keep only the pair of examples from each URL group with the oldest and newest timestamp.
last_index = len(ds) - 1
def check_for_ending_or_beginning_example_in_url_cluster(example, index):
    if index in (last_index, 0):
        return True
    return ds[index-1][args.url_column] != example[args.url_column] or ds[index+1][args.url_column] != example[args.url_column]

ds = ds.filter(lambda example, index: check_for_ending_or_beginning_example_in_url_cluster(example, index), num_proc=args.num_proc, with_indices=True)

# For each example pair, check to see if the text was modified between the old time and the new time.
# If it was modified, keep the latest example and throw the old example out. We have evidence that this new example is up-to-date :D
# If it wasn't modified, throw both examples out. We have no evidence that this new example is up-to-date :(
last_index = len(ds) - 1
def check_for_updated_example_in_url_pair(example, index):
    if index == 0 or ds[index-1][args.url_column] != example[args.url_column]:
        return False
    if ds[index-1][args.text_column] != example[args.text_column]:
        return True
    return False

ds = ds.filter(lambda example, index: check_for_updated_example_in_url_pair(example, index), num_proc=args.num_proc, with_indices=True)

ds.push_to_hub(args.output_dataset_name)
