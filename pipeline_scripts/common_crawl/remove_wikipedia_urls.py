from datasets import load_dataset, load_from_disk
import argparse

parser = argparse.ArgumentParser("Removes all examples from a Hugging Face dataset if they have a Wikipedia URL. This script is intened to be used if you eventually want to merge the dataset with a Wikipedia snapshot. In that case, examples from Wikipedia in this dataset are redundant.")
parser.add_argument("--input_dataset_name", help="Input dataset name.", required=True)
parser.add_argument("--output_dataset_name", help="Output dataset name.", required=True)
parser.add_argument("--url_column", help="Name of the URL column of the dataset.", required=True)
parser.add_argument("--split", default=None, help="The split of the dataset to use. Some datasets don't have splits, so it is optional.")
parser.add_argument("--num_proc", type=int, help="The number of processes to use.")
parser.add_argument("--push_to_hub", action="store_true", help="Whether to push the output dataset to the Hugging Face hub after saving to the disk.")
parser.add_argument("--load_from_hub_instead_of_disk", action="store_true", help="Whether to load the input dataset by name from the Hugging Face hub. If this argument isn't specified then the input dataset will be loaded from a directory of the same name on the disk.")
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

ds = ds.filter(lambda example: not example[args.url_column].startswith("https://en.wikipedia.org/wiki/"), num_proc=args.num_proc)

ds.save_to_disk(args.output_dataset_name)

if args.push_to_hub:
    ds.push_to_hub(args.output_dataset_name)
