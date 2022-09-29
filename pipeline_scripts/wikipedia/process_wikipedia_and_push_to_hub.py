from datasets import load_dataset
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--wikipedia_snapshot")
parser.add_argument("--output_dataset_name")
args = parser.parse_args()

ds = load_dataset("wikipedia", language="en", date=args.wikipedia_snapshot, save_infos=True, beam_runner="DirectRunner")
ds.push_to_hub(args.output_dataset_name)
