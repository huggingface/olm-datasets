from datasets import load_dataset
from dateparser.search import search_dates
import datetime
from statistics import median
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input_dataset_name")
parser.add_argument("--text_column")
parser.add_argument("--num_proc", type=int)
parser.add_argument("--samples", type=int)
args = parser.parse_args()

today = datetime.datetime.today()

if today.month == 1:
    one_month_ago = today.replace(year=today.year - 1, month=12)
else:
    extra_days = 0
    while True:
        try:
            one_month_ago = today.replace(month=today.month - 1, day=today.day - extra_days)
            break
        except ValueError:
            extra_days += 1

one_month_ago = one_month_ago.timestamp()

ds = load_dataset(args.input_dataset_name, split="train")
ds = ds.shuffle(seed=42).select(range(args.samples))

def timestamp_stats(text):
    dates = search_dates(text)
    if dates is None or len(dates) == 0:
        return None
    timestamps = [date_tuple[1].timestamp() for date_tuple in dates]
    avg = sum(timestamps)/len(timestamps)
    fraction_in_past_month = len([timestamp for timestamp in timestamps if timestamp > one_month_ago])/len(timestamps)
    return {"avg": avg, "fraction_in_past_month": fraction_in_past_month}

ds = ds.map(lambda example: {"timestamp_stats": timestamp_stats(example[args.text_column])}, num_proc=args.num_proc)
ds = ds.filter(lambda example: example["timestamp_stats"] is not None, num_proc=args.num_proc)
ds = ds.map(lambda example: {"avg_timestamp": example["timestamp_stats"]["avg"], "timestamp_fraction_in_past_month": example["timestamp_stats"]["fraction_in_past_month"]}, num_proc=args.num_proc)
med = median(ds["avg_timestamp"])
avg = sum(ds["avg_timestamp"])/len(ds["avg_timestamp"])
avg_fraction_in_past_month = sum(ds["timestamp_fraction_in_past_month"])/len(ds["timestamp_fraction_in_past_month"])
print("avg timestamp: ", avg)
print("median timestamp: ", med)
print("avg timestamp fraction in past month: ", avg_fraction_in_past_month)
