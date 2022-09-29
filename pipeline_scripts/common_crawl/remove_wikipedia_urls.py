from datasets import load_dataset
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input_dataset_name")
parser.add_argument("--output_dataset_name")
parser.add_argument("--url_column")
parser.add_argument("--num_proc", type=int)
parser.add_argument("--push_to_hub", action="store_true")
args = parser.parse_args()

ds = load_dataset(args.input_dataset_name)

ds = ds.filter(lambda example: not example[args.url_column].startswith("https://en.wikipedia.org/wiki/"), num_proc=args.num_proc)

ds.save_to_disk(args.output_dataset_name)

if args.push_to_hub:
    ds.push_to_hub(args.output_dataset_name)
