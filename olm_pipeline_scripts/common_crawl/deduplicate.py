from datasets import load_dataset
from text_dedup.embedders.suffix import SuffixArrayEmbedder
from shutil import rmtree
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input_dataset_name")
parser.add_argument("--output_dataset_name")
parser.add_argument("--text_column")
parser.add_argument("--num_proc", type=int)
args = parser.parse_args()

# TODO (Tristan): Make this work without specifying split
ds = load_dataset(args.input_dataset_name, split="train")

# If an example in our courpus has a byte string of 100 or longer which is duplicated elsewhere in the corpus, remove the example.
embedder = SuffixArrayEmbedder(k=100)
slices = embedder.embed_bash(ds[args.text_column])
ds = ds.filter(lambda example, index: slices[index] == [], num_proc=args.num_proc, with_indices=True)

ds.push_to_hub(args.output_dataset_name)
rmtree("cache")  
