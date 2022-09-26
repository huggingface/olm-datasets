from datasets import load_dataset
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input_dataset_name")
parser.add_argument("--output_dataset_name")
parser.add_argument("--url_column")
parser.add_argument("--num_proc", type=int)
args = parser.parse_args()

ds = load_dataset(args.input_dataset_name)

ds = ds.filter(lambda example: not example[args.url_column].startswith("https://en.wikipedia.org/wiki/"), num_proc=args.num_proc)

ds.push_to_hub(args.output_dataset_name)
