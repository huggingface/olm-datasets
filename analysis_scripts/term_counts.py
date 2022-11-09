from datasets import load_dataset, load_from_disk
import argparse
import numpy as np
import matplotlib.pyplot as plt
from collections import Counter
from multiprocessing import Manager
from tqdm import tqdm
from os import path, mkdir
import pickle
import statistics

parser = argparse.ArgumentParser(description="This script takes in an ordered list of datasets and counts terms in each of them, in the specified column. It then plots a graph or a heatmap for how the count changed accross datasets. ")
parser.add_argument("--input_dataset_names", nargs="+", required=True)
parser.add_argument("--input_dataset_pretty_names", nargs="+", required=True, help="The names of the datasets that you want to appear in the saved graph.")
parser.add_argument("--terms", nargs="+", default=None, help="The terms that you want to count. If left as None, then you must specify --num_terms_to_find, and then the script will return the top --num_terms_to_find with the greatest percent change from the first dataset to the last dataset, out of the terms which have count > the mean count plus the standard deviation (so we don't get spurious results from low-count words).")
parser.add_argument("--term_pretty_names", nargs="+", default=None)
parser.add_argument("--analysis_column", required=True)
parser.add_argument("--plot_title", required=True)
parser.add_argument("--split", default=None, help="The dataset split to use. Some datasets don't have splits so this argument is optional.")
parser.add_argument("--num_proc", type=int, required=True)
parser.add_argument("--output_filename", required=True)
parser.add_argument("--as_heatmap", action="store_true")
parser.add_argument("--samples", default=None, type=int)
parser.add_argument("--num_terms_to_find", default=None, type=int)
parser.add_argument("--normalize", action="store_true")
parser.add_argument("--ylabel", required=True)
parser.add_argument("--cache_dir", default="term_count_cache")
parser.add_argument("--load_from_cache_dir", action="store_true")
parser.add_argument("--heatmap_bar_label", default="")
parser.add_argument("--annotation", default=None)
parser.add_argument("--xlabel", default="Dataset")
parser.add_argument("--normalize_axis", default=0, type=int)
parser.add_argument("--percent_increase", action="store_true")
parser.add_argument("--bottom", default=0.25, type=float)
parser.add_argument("--load_from_hub_instead_of_disk", action="store_true", help="Whether to load the input dataset from the Hugging Face hub instead of the disk (default is the disk).")
args = parser.parse_args()

datasets = []
term_y_coords = None
count_dicts = []

if args.load_from_cache_dir:
    if args.terms is None:
        count_dicts = pickle.load(open(path.join(args.cache_dir, "count_dicts.pkl"), "rb"))
    else:
        term_y_coords = pickle.load(open(path.join(args.cache_dir, "term_y_coords.pkl"), "rb"))
    cached_args = pickle.load(open(path.join(args.cache_dir, "args.pkl"), "rb"))
    if args != cached_args:
        print("Warning: argument mismatch between cached args and current args")
        print("Cached args: ", cached_args)
        print("Current args: ", args)

if term_y_coords is None:
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

        datasets.append(ds)

        if args.terms is None and not args.load_from_cache_dir:
            with Manager() as manager:
                shared_list = manager.list()
                def build_count_dict(examples):
                    counts = None
                    for text in examples[args.analysis_column]:
                        if counts is None:
                            counts = Counter(filter(lambda obj: obj.isalpha(), text.lower().split(" ")))
                        else:
                            counts += Counter(filter(lambda obj: obj.isalpha(), text.lower().split(" ")))
                    shared_list.append(counts)

                ds.map(build_count_dict, num_proc=args.num_proc, batched=True, batch_size=len(ds) // args.num_proc, remove_columns=ds.column_names)
        
                count_dict = shared_list[0]
                for counts in tqdm(shared_list[1:]):
                    count_dict += counts

                count_dicts.append(count_dict)

    if args.terms is None:
        if not path.exists(args.cache_dir):
            mkdir(args.cache_dir)
        pickle.dump(args, open(path.join(args.cache_dir, "args.pkl"), "wb"))
        pickle.dump(count_dicts, open(path.join(args.cache_dir, "count_dicts.pkl"), "wb"))

if args.terms is None:

    intersection_count_set = set(count_dicts[0].keys())
    for count_dict in count_dicts[1:]:
        intersection_count_set = intersection_count_set.intersection(set(count_dict.keys()))

    words_with_occurence_changes = []
    counts = []
    for word in intersection_count_set:
        count_sum = 0
        for count_dict in count_dicts:
            count_sum += count_dict[word]
        counts.append(count_sum)
    mean_count = statistics.mean(counts)
    std = statistics.stdev(counts)
    for word in intersection_count_set:
        count_sum = 0
        for count_dict in count_dicts:
            count_sum += count_dict[word]
        if count_sum > mean_count + std:
            change = count_dicts[-1][word]/count_dicts[0][word]
            words_with_occurence_changes.append((word, change))

    words_with_occurence_changes.sort(key=lambda word_and_change: word_and_change[1], reverse=True)
    terms = [word_and_change[0] for word_and_change in words_with_occurence_changes[:args.num_terms_to_find]]

else:
    terms = args.terms


if term_y_coords is None:

    term_y_coords = {term: [] for term in terms}

    for ds in datasets:
        def term_counts(text):
            return {term + "_count": text.lower().count(term.lower()) for term in terms}

        ds = ds.map(lambda example: term_counts(example[args.analysis_column]), num_proc=args.num_proc)

        for term in terms:
            term_y_coords[term].append(sum(ds[term + "_count"]))

    if not path.exists(args.cache_dir):
        mkdir(args.cache_dir)
    pickle.dump(args, open(path.join(args.cache_dir, "args.pkl"), "wb"))
    pickle.dump(term_y_coords, open(path.join(args.cache_dir, "term_y_coords.pkl"), "wb"))

plt.xticks(range(len(args.input_dataset_pretty_names)), args.input_dataset_pretty_names)

if args.as_heatmap:
    matrix = []
    for term in terms:
        matrix.append(term_y_coords[term])
    matrix = np.array(matrix)
    if args.percent_increase:
        matrix = matrix.transpose()
        matrix = (matrix - matrix[0])/matrix[0]
        matrix = matrix.transpose() * 100

    if args.normalize:
        column_sums = matrix.sum(axis=args.normalize_axis)
        if args.normalize_axis == 0:
            normalized_matrix = matrix / column_sums
        if args.normalize_axis == 1:
            normalized_matrix = matrix.transpose() / column_sums
            normalized_matrix = normalized_matrix.transpose()
        plt.imshow(np.flipud(normalized_matrix), plt.cm.Blues)
    else:
        plt.imshow(np.flipud(matrix), plt.cm.Blues)
    plt.yticks(range(len(terms)), reversed(terms if args.term_pretty_names is None else args.term_pretty_names))
    cbar = plt.colorbar()
    cbar.ax.set_ylabel(args.heatmap_bar_label, rotation=-90, va="bottom")
    plt.ylabel(args.ylabel, style='italic', fontweight="bold")
else:
    for term in terms:
        if args.normalize:
            term_y_coords[term] = np.array(term_y_coords[term])/sum(term_y_coords[term])
        plt.plot(term_y_coords[term], label=term, marker=".")
    plt.grid(linestyle=":")
    plt.legend(loc="upper left")
    plt.ylabel(args.ylabel, style='italic', fontweight="bold")

if args.annotation is not None:
    plt.figtext(0.6, 0.01, args.annotation, wrap=True, horizontalalignment='center', fontsize=8)
    plt.subplots_adjust(bottom=args.bottom)
plt.xlabel(args.xlabel, style='italic', fontweight="bold")
plt.title(args.plot_title, fontweight="bold")
plt.rcParams["font.family"] = "Times New Roman"
plt.savefig(args.output_filename, dpi=300)
