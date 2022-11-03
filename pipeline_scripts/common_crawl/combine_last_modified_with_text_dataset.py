from datasets import load_dataset, load_from_disk
import argparse
from multiprocessing import Manager
from tqdm import tqdm
from os import path, mkdir
import pickle
from tqdm import tqdm
from shutil import rmtree

parser = argparse.ArgumentParser(description="This script takes in a text dataset with crawl timestamps and urls, and then a last-modified dataset with crawl timestamps and urls. It uses the shared urls and crawl timestamps to add last-modified timestamps to the text dataset.")
parser.add_argument("--text_dataset_name", required=True)
parser.add_argument("--last_modified_dataset_name", required=True)
parser.add_argument("--output_dataset_name", required=True)
parser.add_argument("--text_dataset_split", default=None)
parser.add_argument("--last_modified_dataset_split", default=None)
parser.add_argument("--last_modified_timestamp_column", required=True)
parser.add_argument("--crawl_timestamp_column", required=True)
parser.add_argument("--url_column", required=True)
parser.add_argument("--num_proc", type=int, required=True)
parser.add_argument("--tmp_dir", default=".tmp_combine_last_modified_with_text_dataset")
parser.add_argument("--load_text_dataset_from_hub_instead_of_disk", action="store_true", help="Whether to load the text dataset from the Hugging Face hub instead of the disk (default is the disk).")
parser.add_argument("--load_last_modified_dataset_from_hub_instead_of_disk", action="store_true", help="Whether to load the last modified dataset from the Hugging Face hub instead of the disk (default is the disk).")
parser.add_argument("--push_to_hub", action="store_true")
args = parser.parse_args()

if path.exists(args.tmp_dir):
    rmtree(args.tmp_dir)

mkdir(args.tmp_dir)

if args.load_text_dataset_from_hub_instead_of_disk:
    if args.text_dataset_split is None:
        text_ds = load_dataset(args.text_dataset_name)
    else:
        text_ds = load_dataset(args.text_dataset_name, split=args.text_dataset_split)
else:
    if args.text_dataset_split is None:
        text_ds = load_from_disk(args.text_dataset_name)
    else:
        text_ds = load_from_disk(args.text_dataset_name)[args.text_dataset_split]

if args.load_last_modified_dataset_from_hub_instead_of_disk:
    if args.last_modified_dataset_split is None:
        last_modified_ds = load_dataset(args.last_modified_dataset_name)
    else:
        last_modified_ds = load_dataset(args.last_modified_dataset_name, split=args.last_modified_dataset_split)
else:
    if args.last_modified_dataset_split is None:
        last_modified_ds = load_from_disk(args.last_modified_dataset_name)
    else:
        last_modified_ds = load_from_disk(args.last_modified_dataset_name)[args.last_modified_dataset_split]

with Manager() as manager:
    shared_list = manager.list()
    def build_last_modified_dict(examples):
        last_modified_dict = {}
        for url, crawl_timestamp, last_modified_tag_timestamp in zip(examples[args.url_column], examples[args.crawl_timestamp_column], examples[args.last_modified_timestamp_column]):
            last_modified_dict[(url, crawl_timestamp)] = last_modified_tag_timestamp
        shared_list.append(last_modified_dict)

    last_modified_ds.map(build_last_modified_dict, num_proc=args.num_proc, batched=True, batch_size=len(last_modified_ds) // args.num_proc)

    aggregate_last_modified_dict = {}
    for last_modified_dict in tqdm(shared_list):
        aggregate_last_modified_dict |= last_modified_dict

    pickle.dump(aggregate_last_modified_dict, open(path.join(args.tmp_dir, "last_modified_dict.pkl"), "wb"))

def add_last_modified(examples):
    last_modified_dict = pickle.load(open(path.join(args.tmp_dir, "last_modified_dict.pkl"), "rb"))
    last_modified_timestamps = []
    for url, crawl_timestamp in zip(examples[args.url_column], examples[args.crawl_timestamp_column]):
        last_modified_timestamps.append(last_modified_dict.get((url, crawl_timestamp), None))
    return {args.last_modified_timestamp_column: last_modified_timestamps}

text_ds = text_ds.map(add_last_modified, num_proc=args.num_proc, batched=True, batch_size=len(text_ds) // args.num_proc)

text_ds.save_to_disk(args.output_dataset_name)

rmtree(args.tmp_dir)

if args.push_to_hub:
    text_ds.push_to_hub(args.output_dataset_name)
