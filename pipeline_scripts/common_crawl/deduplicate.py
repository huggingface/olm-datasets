from datasets import load_dataset
from text_dedup.exact_dedup import GoogleSuffixArrayDeduplicator
from shutil import rmtree
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input_dataset_name")
parser.add_argument("--output_dataset_name")
parser.add_argument("--text_column")
parser.add_argument("--split")
parser.add_argument("--num_proc", type=int)
parser.add_argument("--push_to_hub", action="store_true")
args = parser.parse_args()

ds = load_dataset(args.input_dataset_name, split=args.split)

# If an example in our courpus has a byte string of 100 or longer which is duplicated elsewhere in the corpus, remove the example.
deduplicator = GoogleSuffixArrayDeduplicator(k=100)
slices = deduplicator.fit_predict(ds[args.text_column])
ds = ds.filter(lambda example, index: slices[index] == [], num_proc=args.num_proc, with_indices=True)

ds.save_to_disk(args.output_dataset_name)
rmtree("cache")

if args.push_to_hub:
    ds.push_to_hub(args.output_dataset_name)
