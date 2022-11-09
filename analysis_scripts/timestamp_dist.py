from datasets import load_dataset, load_from_disk
import argparse
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from datetime import datetime
from os import path, mkdir
import pickle

parser = argparse.ArgumentParser(description="This script takes in an ordered list of datasets. It is assumed that each dataset has a timestamp column. The script plots the timestamp distribution histogram for each dataset.")
parser.add_argument("--input_dataset_names", nargs="+", required=True)
parser.add_argument("--input_dataset_pretty_names", nargs="+", required=True, help="The names of the datasets that you want to appear in the saved graph.")
parser.add_argument("--timestamp_column", required=True)
parser.add_argument("--plot_title", required=True)
parser.add_argument("--split", default=None, help="The dataset split to use. Some datasets don't have splits so this argument is optional.")
parser.add_argument("--num_proc", type=int, required=True)
parser.add_argument("--output_filename", required=True)
parser.add_argument("--samples", default=None, type=int)
parser.add_argument("--bins", default=100, help="The number of histogram bins to plot")
parser.add_argument("--cache_dir", default="timestamp_dist_cache")
parser.add_argument("--load_from_cache_dir", action="store_true")
parser.add_argument("--annotation", default=None)
parser.add_argument("--legend_title", default="Internet Snapshot")
parser.add_argument("--load_from_hub_instead_of_disk", action="store_true", help="Whether to load the input dataset from the Hugging Face hub instead of the disk (default is the disk).")
args = parser.parse_args()

if args.load_from_cache_dir:
    data_array = np.load(open(path.join(args.cache_dir, "data_array.npy"), "rb"))
    cached_args = pickle.load(open(path.join(args.cache_dir, "args.pkl"), "rb"))
    if args != cached_args:
        print("Warning: argument mismatch between cached args and current args")
        print("Cached args: ", cached_args)
        print("Current args: ", args)
else:

    # Remove timestamp outliers more than 10 median deviations away from the median.
    # This is important if the timestamp is the Last-Modified timestamp, which can sometimes be wrong
    # because websites can report whatever they want. We don't want one website that says it was created
    # a billion years ago to seriously affect the distribution.
    def reject_outliers(data, m = 10.):
        d = np.abs(data - np.median(data))
        mdev = np.median(d)
        s = d/mdev if mdev else 0.
        return data[s<m]

    data_list = []
    shortest_len = None
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

        ds = ds.filter(lambda example: example[args.timestamp_column] is not None, num_proc=args.num_proc)
        
        data = np.array(ds[args.timestamp_column])
        data_no_outliers = reject_outliers(data)
        data_list.append(data_no_outliers)
        if shortest_len is None:
            shortest_len = len(data_no_outliers)
        else:
            shortest_len = min(shortest_len, len(data_no_outliers))

    truncated_data_list = []
    for data in data_list:
        truncated_data_list.append(data[:shortest_len])
    data_array = np.array(truncated_data_list).transpose()
    if not path.exists(args.cache_dir):
        mkdir(args.cache_dir)
    np.save(open(path.join(args.cache_dir, "data_array.npy"), "wb"), data_array)
    pickle.dump(args, open(path.join(args.cache_dir, "args.pkl"), "wb"))

df = pd.DataFrame(data=data_array, columns=args.input_dataset_pretty_names)
color_palette = sns.color_palette("viridis")
colors = color_palette[:len(args.input_dataset_names)]
plot = sns.displot(data=df, kde=True, palette=colors, bins=args.bins, height=5, aspect=1.5)
means = np.mean(data_array, axis=0)
xticks = np.concatenate((np.array([np.min(data_array)]), means, np.array([np.max(data_array)])))
for mean, color in zip(means, colors):
    plt.axvline(x=mean, linestyle="--", color=color)

plot.set(xticks=xticks)
plot.axes[0,0].set_title(args.plot_title, fontweight="bold")
plot.axes[0,0].set_xlabel("Timestamp", style="italic", fontweight="bold")
plot.axes[0,0].set_ylabel("Count", style="italic", fontweight="bold")
plot.set_xticklabels([datetime.fromtimestamp(timestamp).strftime('%b %d') for timestamp in xticks], rotation=45)
if args.annotation is not None:
    plot.figure.text(0.5, 0.01, args.annotation, wrap=True, horizontalalignment='center', fontsize=8)
    plot.figure.subplots_adjust(bottom=0.20)
plot._legend.set_title(args.legend_title)
plot.fig.savefig(args.output_filename, dpi=300, bbox_inches='tight')

