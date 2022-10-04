from datasets import load_dataset, load_from_disk
from dateparser.search import search_dates
import datetime
from statistics import median
import argparse

parser = argparse.ArgumentParser(description="This script analyzes a sample of a dataset for which dates are mentioned in the text.")
parser.add_argument("--input_dataset_name", required=True)
parser.add_argument("--text_column", required=True)
parser.add_argument("--split", default=None, help="The dataset split to use. Some datasets don't have splits so this argument is optional.")
parser.add_argument("--num_proc", type=int, required=True)
parser.add_argument("--samples", type=int, default=1000)
parser.add_argument("--load_from_hub_instead_of_disk", action="store_true", help="Whether to load the input dataset from the Hugging Face hub instead of the disk (default is the disk).")
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

today = datetime.datetime.today()
one_week_ago = (today-datetime.timedelta(weeks=1)).timestamp()
one_month_ago = (today-datetime.timedelta(weeks=4)).timestamp()
two_months_ago = (today-datetime.timedelta(weeks=8)).timestamp()
three_months_ago = (today-datetime.timedelta(weeks=12)).timestamp()
times_to_compare = {"one_month_ago": one_month_ago, "two_months_ago": two_months_ago, "three_months_ago": three_months_ago}

ds = ds.shuffle(seed=42).select(range(args.samples))

def timestamp_stats(text):
    try:
        dates = search_dates(text, languages=["en"])
    except Exception:
        return None
    if dates is None or len(dates) == 0:
        return None
    timestamps = [date_tuple[1].timestamp() for date_tuple in dates]
    stats = {"timestamps": timestamps}
    for name, reference_timestamp in times_to_compare.items():
        # Don't include dates from more recent than a week ago, because the parser parses terms like "Today" in the text as the current date.
        is_time_after = 1 if len([timestamp for timestamp in timestamps if timestamp > reference_timestamp and timestamp < one_week_ago]) > 0 else 0
        stats["is_time_after_" + name] = is_time_after
    return stats

ds = ds.map(lambda example: {"timestamp_stats": timestamp_stats(example[args.text_column])}, num_proc=args.num_proc)
ds = ds.filter(lambda example: example["timestamp_stats"] is not None, num_proc=args.num_proc)
ds = ds.map(lambda example: example["timestamp_stats"], num_proc=args.num_proc, remove_columns=[name for name in ds.column_names if name not in ds[0]["timestamp_stats"].keys()])

for column in ds.column_names:
    if column != "timestamps":
        med = median(ds[column])
        avg = sum(ds[column])/len(ds[column])
        print(column + " avg: ", avg)

all_timestamps = []
for timestamps in ds["timestamps"]:
    all_timestamps += timestamps

print("avg timestamp: ", sum(all_timestamps)/len(all_timestamps))
print("median timestamp: ", median(all_timestamps))
