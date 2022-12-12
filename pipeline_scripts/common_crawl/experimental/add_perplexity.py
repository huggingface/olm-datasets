from datasets import load_dataset, load_from_disk
import argparse
import sys
sys.path.append("kenlm")
from model import KenlmModel

parser = argparse.ArgumentParser(description="This script simply uses a kenlm trained on English Wikipedia to compute the perplexity of each text example in the dataset. It then sorts the dataset by perplexity so that the user can then select the range of perplexities that they want their data to be in.")
parser.add_argument("--input_dataset_name", help="The name of the input dataset.", required=True)
parser.add_argument("--output_dataset_name", help="The name of the output dataset.", required=True)
parser.add_argument("--split", default=None, help="The split of the dataset to apply the filters to. Not all datasets have splits, so this is not a required argument.")
parser.add_argument("--text_column", help="The name of the dataset column that contains the text.", required=True)
parser.add_argument("--num_proc", type=int, help="The number of processes to use.", required=True)
parser.add_argument("--push_to_hub", action="store_true", help="Whether to push the output dataset to the Hugging Face Hub after saving it to the disk.")
parser.add_argument("--load_from_hub_instead_of_disk", action="store_true", help="Whether to pull the input dataset by name from the Hugging Face Hub. If this argument is not used, it is assumed that there is a dataset saved to the disk with the input dataset name.")
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


model = KenlmModel.from_pretrained("kenlm/wikipedia", "en")
ds = ds.map(lambda example: {"kenlm_ppl": model.get_perplexity(example[args.text_column])}, num_proc=args.num_proc)
ds = ds.sort("kenlm_ppl")
ds.save_to_disk(args.output_dataset_name)

if args.push_to_hub:
    ds.push_to_hub(args.output_dataset_name)
