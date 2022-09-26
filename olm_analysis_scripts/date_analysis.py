from datasets import load_dataset
from dateparser.search import search_dates
from statistics import median
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input_dataset")
parser.add_argument("--output_dataset")
parser.add_argument("--text_column")
args = parser.parse_args()

ds = load_dataset(args.input_dataset)

def avg_timestamp_in_text(text):
    timestamps = [date_tuple[1].timestamp() for date_tuple in search_dates(example[args.text_column])]
    if len(timestamps) == 0:
        return None
    return sum(timestamps)/len(timestamps)

ds = ds.map(lambda example: {"avg_timestamp": avg_timestamp_in_text(example[args.text_column])}, num_proc=args.num_proc)
ds = ds.filter(lambda example: example["avg_timestamp"] is not None, num_proc=args.num_proc)
med = median(ds["avg_timestamp"])
avg = sum(ds["avg_timestamp"])/len(ds["avg_timestamp"])
print("avg timestamp: ", avg)
print("median timestamp: ", med)


