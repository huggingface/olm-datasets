from datasets import load_dataset, load_from_disk
import argparse
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description="This script takes in an ordered list of datasets and counts the specified terms in each of them, in the specified column. It then saves a plot of lines where y is the count of terms and x is the datasets.")
parser.add_argument("--input_dataset_names", nargs="+", required=True)
parser.add_argument("--input_dataset_pretty_names", nargs="+", required=True, help="The names of the datasets that you want to appear in the saved graph.")
parser.add_argument("--terms", nargs="+", required=True)
parser.add_argument("--case_sensitive", default=False)
parser.add_argument("--analysis_column", required=True)
parser.add_argument("--plot_title", required=True)
parser.add_argument("--split", default=None, help="The dataset split to use. Some datasets don't have splits so this argument is optional.")
parser.add_argument("--num_proc", type=int, required=True)
parser.add_argument("--output_filename", required=True)
parser.add_argument("--load_from_hub_instead_of_disk", action="store_true", help="Whether to load the input dataset from the Hugging Face hub instead of the disk (default is the disk).")
args = parser.parse_args()

term_y_coords = {term: [] for term in args.terms}

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

    def term_counts(text):
        if args.case_sensitive:
            return {term + "_count": text.count(term) for term in args.terms}
        return {term + "_count": text.lower().count(term.lower()) for term in args.terms}

    ds = ds.map(lambda example: term_counts(example[args.analysis_column]), num_proc=args.num_proc)

    for term in args.terms:
        term_y_coords[term].append(sum(ds[term + "_count"]))

for term in args.terms:
    plt.plot(term_y_coords[term], label=term, marker=".")

plt.xticks(range(len(args.input_dataset_pretty_names)), args.input_dataset_pretty_names)
plt.grid(linestyle=":")
plt.legend(loc="upper left")
plt.ylabel("Count of Term", style='italic', fontweight="bold")
plt.xlabel("Dataset", style='italic', fontweight="bold")
plt.title(args.plot_title, fontweight="bold")
plt.rcParams["font.family"] = "Times New Roman"

plt.savefig(args.output_filename, dpi=300)
