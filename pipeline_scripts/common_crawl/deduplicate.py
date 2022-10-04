from datasets import load_dataset, load_from_disk
from text_dedup.exact_dedup import GoogleSuffixArrayDeduplicator
from shutil import rmtree
from os import path
import argparse
import uuid

parser = argparse.ArgumentParser(description="Applies exact suffix array deduplication to a Hugging Face dataset.")
parser.add_argument("--input_dataset_name", help="Name of the input dataset.", required=True)
parser.add_argument("--output_dataset_name", help="Name of the output dataset.", required=True)
parser.add_argument("--text_column", help="Name of the dataset's text column.", required=True)
parser.add_argument("--split", default=None, help="The split of the dataset to apply deduplication on. Not all datasets have splits, so this argument is optional.")
parser.add_argument("--num_proc", type=int, help="The minimum number of processes to use.", required=True)
parser.add_argument("--push_to_hub", action="store_true", help="Whether to push the output dataset to the Hugging Face Hub after saving it to the disk.")
parser.add_argument("--load_from_hub_instead_of_disk", action="store_true", help="Whether to load the input dataset from the Hugging Face Hub. If this argument is not used, then it is assumed that the input dataset is stored locally on the disk.")
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

# Sort the dataset so examples with the same first 200 bytes of text are grouped together.
print("Sorting by first 100 bytes of text")
temp_column_name = str(uuid.uuid4())
ds = ds.map(lambda example: {temp_column_name: example[args.text_column][:200]}, num_proc=args.num_proc)
ds = ds.sort(temp_column_name)

# Filter away examples if their first 200 bytes of text exactly matches another example's first 200 bytes of text.
# This gets rid of a subset of the examples that the next step (suffix array deduplication) gets rid of, so we technically
# don't need to do it. But it speeds up the next step quite a bit to do this first.
last_index = len(ds) - 1
ds = ds.filter(lambda example, index: check_for_ending_example_in_cluster(example, index, temp_column_name, last_index), num_proc=args.num_proc, with_indices=True)
ds = ds.remove_columns(temp_column_name)

# Now, do an aggressive form of Suffix Array Substring Exact Deduplication.
# If an example in our courpus has a byte string of 200 or longer which is duplicated elsewhere in the corpus, remove the whole example.
# In the paper for this deduplication method, they only remove the byte string, not the whole example. Removing the whole example will vastly
# shrink the size of the dataset, but it will ensure better quality data, without gaps in text continuity. We have plenty of data, so we opt for
# removing the whole example.
if path.exists(".cache"):
    rmtree(".cache")

deduplicator = GoogleSuffixArrayDeduplicator(k=200)

# We need to create this iterator over the dataset text column
# to ensure that not all of the text entries are loaded into memory at once.
class DatasetColumnIterator():
    def __init__(self, dataset, column):
        self.iterable_dataset = dataset.__iter__()
        self.column = column

    def __iter__(self):
        return self

    def __next__(self):
        return self.iterable_dataset.__next__()[self.column]

slices = deduplicator.fit_predict(DatasetColumnIterator(ds, args.text_column))
ds = ds.filter(lambda example, index: slices[index] == [], num_proc=args.num_proc, with_indices=True)

ds.save_to_disk(args.output_dataset_name)
rmtree(".cache")

if args.push_to_hub:
    ds.push_to_hub(args.output_dataset_name)
