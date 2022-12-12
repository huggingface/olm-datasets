from datasets import load_dataset, load_from_disk, concatenate_datasets
from text_dedup.exact_dedup import GoogleSuffixArrayDeduplicator
from shutil import rmtree
from os import path
import argparse
import hashlib
import uuid

parser = argparse.ArgumentParser(description="Applies varying levels of exact deduplication or exact suffix array deduplication to a Hugging Face dataset.")
parser.add_argument("--input_dataset_name", help="Name of the input dataset.", required=True)
parser.add_argument("--output_dataset_name", help="Name of the output dataset.", required=True)
parser.add_argument("--text_column", help="Name of the dataset's text column.", required=True)
parser.add_argument("--split", default=None, help="The split of the dataset to apply deduplication on. Not all datasets have splits, so this argument is optional.")
parser.add_argument("--num_proc", type=int, help="The minimum number of processes to use.", required=True)
parser.add_argument("--push_to_hub", action="store_true", help="Whether to push the output dataset to the Hugging Face Hub after saving it to the disk.")
parser.add_argument("--remove_whole_example", action="store_true", help= "If an example in our courpus has a byte string of 100 or longer which is duplicated elsewhere in the corpus, then this option will result in the removal of the whole example. If this option is not specified, then only the substring is removed, not the whole example. In the paper for this deduplication method, they only remove the byte string, not the whole example. Removing the whole example will vastly shrink the size of the dataset, but it will ensure no gaps in text continuity.")
parser.add_argument("--only_exact_duplicates", action="store_true", "Use this option if you want to forget about the suffix array stuff and just get rid of examples that exactly match other examples in the dataset." )
parser.add_argument("--chunks", type=int, default=1, help="Deduplication can be really memory-intensive. This option allows you to split the dataset up in to n chunks, and perform deduplication independently on each of the chunks. Then the resulting deduplicated datasets are concatenated together at the end.")
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

deduplicated_ds_shard_list = []
for ds_shard_index in range(args.chunks):
    ds_shard = ds.shard(num_shards=args.chunks, index=ds_shard_index)
    
    if args.remove_whole_example:
        def check_for_ending_example_in_cluster(example, index, column, last_index):
            if index == last_index:
                return True
            return ds_shard[index+1][column] != example[column]

        # Sort the dataset so examples with the same first 100 bytes of text are grouped together.
        print("Sorting by first 100 bytes of text")
        temp_column_name = str(uuid.uuid4())
        ds_shard = ds_shard.map(lambda example: {temp_column_name: example[args.text_column].encode("u8")[:100]}, num_proc=args.num_proc)
        ds_shard = ds_shard.sort(temp_column_name)

        # Filter away examples if their first 100 bytes of text exactly matches another example's first 100 bytes of text.
        # This gets rid of a subset of the examples that the next step (suffix array deduplication) gets rid of, so we technically
        # don't need to do it. But it speeds up the next step quite a bit to do this first.
        last_index = len(ds_shard) - 1
        len_before = len(ds_shard)
        ds_shard = ds_shard.filter(lambda example, index: check_for_ending_example_in_cluster(example, index, temp_column_name, last_index), num_proc=args.num_proc, with_indices=True)
        ds_shard = ds_shard.remove_columns(temp_column_name)
        print(f"Got rid of all examples sharing first 100 bytes of text, as a speedup step. Removed {len_before - len(ds_shard)} from {len_before} examples.")

        # Do the same thing with the ending 100 bytes of text.
        print("Sorting by last 100 bytes of text")
        temp_column_name = str(uuid.uuid4())
        ds_shard = ds_shard.map(lambda example: {temp_column_name: example[args.text_column].encode("u8")[-100:]}, num_proc=args.num_proc)
        ds_shard = ds_shard.sort(temp_column_name)

        last_index = len(ds_shard) - 1 
        len_before = len(ds_shard)
        ds_shard = ds_shard.filter(lambda example, index: check_for_ending_example_in_cluster(example, index, temp_column_name, last_index), num_proc=args.num_proc, with_indices=True)
        ds_shard = ds_shard.remove_columns(temp_column_name)
        print(f"Got rid of all examples sharing last 100 bytes of text, as a speedup step. Removed {len_before - len(ds_shard)} from {len_before} examples.") 

    else:
        print("Getting rid of exact duplicates")
        def check_for_ending_example_in_cluster(example, index, column, last_index):
            if index == last_index:
                return True
            return ds_shard[index+1][column] != example[column]

        temp_column_name = str(uuid.uuid4())
        ds_shard = ds_shard.map(lambda example: {temp_column_name: hashlib.md5(example[args.text_column].encode()).hexdigest()}, num_proc=args.num_proc)
        ds_shard = ds_shard.sort(temp_column_name)

        last_index = len(ds_shard) - 1
        ds_shard = ds_shard.filter(lambda example, index: check_for_ending_example_in_cluster(example, index, temp_column_name, last_index), num_proc=args.num_proc, with_indices=True)
        ds_shard = ds_shard.remove_columns(temp_column_name)
        print("Got rid of exact duplicates")

    if path.exists(".cache"):
        rmtree(".cache")

    if not args.only_exact_duplicates:
        # Now, do Suffix Array Substring Exact Deduplication.

        deduplicator = GoogleSuffixArrayDeduplicator(k=100)

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

        slices = deduplicator.fit_predict(DatasetColumnIterator(ds_shard, args.text_column))
        if args.remove_whole_example:
            ds_shard = ds_shard.filter(lambda example, index: slices[index] == [], num_proc=args.num_proc, with_indices=True)
        else:
            def remove_slice_list(string, slice_list):
                for s in slice_list:
                    string = string.replace(string[s], "")
                return string
            # It's important to give this map function a uuid as its fingerprint. If we let it compute the fingerprint as a hash of the whole slice_list, then it will take too long.
            ds_shard = ds_shard.map(lambda example, index: {args.text_column: remove_slice_list(example[args.text_column], slices[index])}, num_proc=args.num_proc, with_indices=True, new_fingerprint=str(uuid.uuid4()))
            ds_shard = ds_shard.filter(lambda example: example[args.text_column] != "", num_proc=args.num_proc)

    if path.exists(".cache"):
        rmtree(".cache")

    deduplicated_ds_shard_list.append(ds_shard)

ds = concatenate_datasets(deduplicated_ds_shard_list)

ds.save_to_disk(args.output_dataset_name)

if args.push_to_hub:
    ds.push_to_hub(args.output_dataset_name)
