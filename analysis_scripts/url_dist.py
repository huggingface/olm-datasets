from datasets import load_dataset, load_from_disk
import argparse
from collections import Counter
from multiprocessing import Manager
from tqdm import tqdm
from urllib.parse import urlparse
import seaborn as sns
import pandas as pd
from os import path, mkdir
import pickle

parser = argparse.ArgumentParser(description="This script takes in an ordered list of datasets which each have a URL column. It extracts domain names from each URL and then plots a histogram of the URL counts per domain and a correlation matrix comparing each dataset's histogram.")
parser.add_argument("--input_dataset_names", nargs="+", required=True)
parser.add_argument("--input_dataset_pretty_names", nargs="+", required=True, help="The names of the datasets that you want to appear in the saved graphs.")
parser.add_argument("--url_column", required=True)
parser.add_argument("--hist_plot_title", required=True)
parser.add_argument("--corr_plot_title", required=True)
parser.add_argument("--split", default=None, help="The dataset split to use. Some datasets don't have splits so this argument is optional.")
parser.add_argument("--num_proc", type=int, required=True)
parser.add_argument("--output_corr_filename", required=True)
parser.add_argument("--output_hist_filename", required=True)
parser.add_argument("--samples", default=None, type=int)
parser.add_argument("--hist_bins", default=25, type=int)
parser.add_argument("--hist_bin_fontsize", default=8, type=int)
parser.add_argument("--cache_dir", default="url_dist")
parser.add_argument("--no_hist_legend", action="store_true")
parser.add_argument("--load_from_cache_dir", action="store_true", help="If you've already run this function and just want to change parameters for the graphs (like --hist_bins, for example), then specify this option to load the cached domain distribution so the computation isn't repeated.")
parser.add_argument("--annotation", default=None)
parser.add_argument("--load_from_hub_instead_of_disk", action="store_true", help="Whether to load the input datasets from the Hugging Face hub instead of the disk (default is the disk).")
args = parser.parse_args()

if args.load_from_cache_dir:
    count_dicts = pickle.load(open(path.join(args.cache_dir, "count_dicts.pkl"), "rb"))
    cached_args = pickle.load(open(path.join(args.cache_dir, "args.pkl"), "rb"))
    if args != cached_args:
        print("Warning: argument mismatch between cached args and current args")
        print("Cached args: ", cached_args)
        print("Current args: ", args)

else:
    count_dicts = []
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

        if args.samples is not None:
            ds = ds.shuffle(seed=42)
            ds = ds.select(range(args.samples))

        with Manager() as manager:
            shared_list = manager.list()
            def build_count_dict(examples):
                counts = None
                for url in examples[args.url_column]:
                    domain = urlparse(url).netloc
                    if counts is None:
                        counts = Counter([domain])
                    else:
                        counts += Counter([domain])
                shared_list.append(counts)

            ds.map(build_count_dict, num_proc=args.num_proc, batched=True, batch_size=len(ds) // args.num_proc)
        
            count_dict = shared_list[0]
            for counts in tqdm(shared_list[1:]):
                count_dict += counts

            count_dicts.append(count_dict)
    
    if not path.exists(args.cache_dir):
        mkdir(args.cache_dir)
    pickle.dump(args, open(path.join(args.cache_dir, "args.pkl"), "wb"))
    pickle.dump(count_dicts, open(path.join(args.cache_dir, "count_dicts.pkl"), "wb"))

union_count_set = set(count_dicts[0].keys())
for count_dict in tqdm(count_dicts[1:]):
    union_count_set = union_count_set.union(set(count_dict.keys()))

dataframe_dict = {dataset_name: [] for dataset_name in args.input_dataset_pretty_names}
dataframe_dict["domain_name"] = []
for domain in tqdm(union_count_set):
    for index in range(len(args.input_dataset_pretty_names)):
        count_dict = count_dicts[index]
        dataset_name = args.input_dataset_pretty_names[index]
        dataframe_dict[dataset_name].append(count_dict.get(domain, 0))
    dataframe_dict["domain_name"].append(domain)

df = pd.DataFrame(dataframe_dict)
plot = sns.heatmap(df.corr().iloc[::-1], cmap="Blues", annot=True)
plot.set_title(args.corr_plot_title, fontweight="bold")
if args.annotation is not None:
    plot.figure.text(0.5, 0.01, args.annotation, wrap=True, horizontalalignment="center", fontsize=8)
    plot.figure.subplots_adjust(bottom=0.15)
plot.figure.savefig(args.output_corr_filename, dpi=300)
plot.figure.clf()

df = df.sort_values(by=args.input_dataset_pretty_names, ascending=False)
dataframe_dict = {"samples": [], "dataset": [], "domain": []}
index = 0
for _, datum in df.iterrows():
    if index >= args.hist_bins:
        break
    dataframe_dict["samples"] += [datum[name] for name in args.input_dataset_pretty_names]
    dataframe_dict["dataset"] += args.input_dataset_pretty_names
    dataframe_dict["domain"] += [datum["domain_name"]]*len(args.input_dataset_pretty_names)
    index += 1

df = pd.DataFrame(dataframe_dict)
color_palette = sns.color_palette("pastel")
colors = color_palette[:len(args.input_dataset_names)]
plot = sns.barplot(data=df, palette=colors, hue="dataset", y="domain", x="samples")
if args.annotation is not None:
    plot.figure.text(0.4, 0.01, args.annotation, wrap=True, horizontalalignment="center", fontsize=8)
    plot.figure.subplots_adjust(bottom=0.15)
plot.legend().set_title("")
if args.no_hist_legend:
    plot.legend().remove()
for item in plot.get_yticklabels():
    item.set_fontsize(args.hist_bin_fontsize)
plot.set_title(args.hist_plot_title, fontweight="bold")
plot.set_xlabel("Count", style="italic", fontweight="bold")
plot.set_ylabel("Domain", style="italic", fontweight="bold")
plot.figure.savefig(args.output_hist_filename, dpi=300, bbox_inches="tight")
