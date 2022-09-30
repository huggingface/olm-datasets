from datasets import load_dataset, load_from_disk
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input_dataset_name")
parser.add_argument("--output_dataset_name")
parser.add_argument("--url_column")
parser.add_argument("--split")
parser.add_argument("--num_proc", type=int)
parser.add_argument("--push_to_hub", action="store_true")
parser.add_argument("--load_from_hub_instead_of_disk", action="store_true")
args = parser.parse_args()

if args.load_from_hub_instead_of_disk:
    ds = load_dataset(args.input_dataset_name, split=args.split)
else:
    ds = load_from_disk(args.input_dataset_name)[args.split]

ds = ds.filter(lambda example: not example[args.url_column].startswith("https://en.wikipedia.org/wiki/"), num_proc=args.num_proc)

ds.save_to_disk(args.output_dataset_name)

if args.push_to_hub:
    ds.push_to_hub(args.output_dataset_name)
