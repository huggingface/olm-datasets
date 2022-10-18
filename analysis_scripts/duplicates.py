from datasets import load_dataset, load_from_disk, concatenate_datasets
import argparse
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description="This script takes a list of datasets, concatenates them, and saves a pie chart for duplicate versus unique items in the specified column.")
parser.add_argument("--input_dataset_names", nargs="+", required=True)
parser.add_argument("--analysis_column", required=True)
parser.add_argument("--plot_title", required=True)
parser.add_argument("--split", default=None, help="The dataset split to use. Some datasets don't have splits so this argument is optional.")
parser.add_argument("--num_proc", type=int, required=True)
parser.add_argument("--duplicate_label", default="Duplicate")
parser.add_argument("--unique_label", default="Unique")
parser.add_argument("--output_filename", required=True)
parser.add_argument("--load_from_hub_instead_of_disk", action="store_true", help="Whether to load the input dataset from the Hugging Face hub instead of the disk (default is the disk).")
args = parser.parse_args()

datasets = []
for input_dataset_name in args.input_dataset_names:
    if args.load_from_hub_instead_of_disk:
        if args.split is None:
            ds = load_dataset(input_dataset_name)
        else:
            ds = load_dataset(input_dataset_name, split=args.split)
    else:
        if args.split is None:
            ds = load_from_disk(input_dataset_name)
        else:
            ds = load_from_disk(input_dataset_name)[args.split]
    
    datasets.append(ds)

ds = concatenate_datasets(datasets)

ds = ds.sort(args.analysis_column)

max_index = len(ds) - 1
def same_adjacent_entry(entry, index):
    if index == max_index:
        return ds[index - 1][args.analysis_column] == entry
    elif index == 0:
        return ds[index + 1][args.analysis_column] == entry
    return ds[index - 1][args.analysis_column] == entry or ds[index + 1][args.analysis_column] == entry

num_examples = len(ds)
ds = ds.filter(lambda example, index: same_adjacent_entry(example[args.analysis_column], index), num_proc=args.num_proc, with_indices=True)
num_examples_only_duplicate_entries = len(ds)


labels = [args.duplicate_label, args.unique_label]
sizes = [num_examples_only_duplicate_entries, num_examples - num_examples_only_duplicate_entries]
plt.pie(sizes, labels=labels, autopct='%1.1f%%')

plt.title(args.plot_title, fontweight="bold")
plt.rcParams["font.family"] = "Times New Roman"

plt.savefig(args.output_filename, dpi=300)
