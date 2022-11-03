from datasets import load_dataset
from tqdm import tqdm
import pandas as pd
import subprocess
from multiprocessing import Process
from os import walk, mkdir, path
from shutil import rmtree
import dateutil
import dateparser
import argparse
import ujson

parser = argparse.ArgumentParser(description="Turns WAT downloads from download_common_crawl.py into a Hugging Face dataset with Last-Modified timestamps, URLs, and crawl timestamps.")
parser.add_argument("--download_dir", help="The directory of the downloaded WAT files.", required=True)
parser.add_argument("--output_dataset_name", help="The name of the Hugging Face dataset which will be saved upon completion of this program.", required=True)
parser.add_argument("--num_proc", type=int, help="The number of processes to use.", required=True)
parser.add_argument("--tmp_dir", default=".tmp_get_last_modified_dataset_from_wat_downloads")
parser.add_argument("--push_to_hub", action="store_true", help="Whether to push the Hugging Face dataset to the Hugging Face Hub after saving a copy to the disk.")
args = parser.parse_args()

if path.exists(args.tmp_dir):
    rmtree(args.tmp_dir)

mkdir(args.tmp_dir)

filenames = next(walk(args.download_dir), (None, None, []))[2]

def split_a_into_n_parts(a, n):
    k, m = divmod(len(a), n)
    return [a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n)]

filename_per_proc = [names for names in split_a_into_n_parts(filenames, args.num_proc) if len(names) != 0]

processes = []
for filenames in filename_per_proc:
    def get_dataset(filenames):
        for filename in tqdm(filenames):
            dataset_dict = {"last_modified_timestamp": [], "url": [], "crawl_timestamp": []}
            file_path = path.join(args.download_dir, filename)
            if filename.endswith(".gz"):
                subprocess.run(f"gzip -d {file_path}", shell=True)
                filename = filename[:-3]
                file_path = path.join(args.download_dir, filename)
            for line in open(file_path).readlines():
                if line.startswith("{"):
                    parsed_line = ujson.loads(line)
                    last_modified = parsed_line.get("Envelope", {}).get("Payload-Metadata", {}).get("HTTP-Response-Metadata", {}).get("Headers", {}).get("Last-Modified", None)
                    url = parsed_line.get("Envelope", {}).get("WARC-Header-Metadata", {}).get("WARC-Target-URI", None)
                    date = parsed_line.get("Envelope", {}).get("WARC-Header-Metadata", {}).get("WARC-Date", None)
                    if None not in (last_modified, url, date):
                        try:
                            last_modified_timestamp = dateutil.parser.parse(last_modified).timestamp()
                        except Exception:
                            try:
                                last_modified_timestamp = dateparser.parse(last_modified).timestamp()
                            except Exception:
                                last_modified_timestamp = None
                        if last_modified_timestamp is not None:
                            crawl_timestamp = dateutil.parser.parse(date).timestamp()
                            dataset_dict["last_modified_timestamp"].append(last_modified_timestamp)
                            dataset_dict["url"].append(url)
                            dataset_dict["crawl_timestamp"].append(crawl_timestamp)
            # Zip the download file again to save space.
            subprocess.run(f"gzip {file_path}", shell=True)
            pd.DataFrame(dataset_dict).to_parquet(path.join(args.tmp_dir, filename + ".filtered.parquet"))
    p = Process(target=get_dataset, args=(filenames,))
    p.start()
    processes.append(p)

for p in processes:
    p.join()

ds = load_dataset("parquet", data_files=path.join(args.tmp_dir, "*.parquet"))
ds.save_to_disk(args.output_dataset_name)

rmtree(args.tmp_dir)

if args.push_to_hub:
    ds.push_to_hub(args.output_dataset_name)

