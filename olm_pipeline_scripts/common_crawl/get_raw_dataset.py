from datasets import Dataset
from tqdm import tqdm
import pandas as pd
import subprocess
from multiprocessing import Process
from os import walk, mkdir, path
from shutil import move, rmtree
import dateutil
import argparse

# TODO (Tristan): Make this not change the formatting of the download dir.
parser = argparse.ArgumentParser()
parser.add_argument("--common_crawl_download_dir")
parser.add_argument("--output_dataset_name")
parser.add_argument("--num_proc", type=int)
args = parser.parse_args()

filenames = next(walk(args.common_crawl_download_dir), (None, None, []))[2]

def split_a_into_n_parts(a, n):
    k, m = divmod(len(a), n)
    return [a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n)]

ungoliant_pipeline_output_dirs = []
filename_per_directory = [names for names in split_a_into_n_parts(filenames, args.num_proc) if len(names) != 0]
num_files_awaiting_processing = 0
dirs_awaiting_processing = []
def do_parallel_pipeline_processing(dirs_awaiting_processing):
    processes = []
    for obj in dirs_awaiting_processing:
        p = subprocess.Popen(f"ungoliant pipeline --lid-path=sp_kenlm_ft_models/lid.176.bin {obj['download_chunk_dir']} {obj['pipeline_output_dir']}", shell=True)
        processes.append(p)
    for p in processes:
        p.wait()
mkdir("ungoliant_pipeline_results")
for i in range(len(filename_per_directory)):
    download_chunk_dir = args.common_crawl_download_dir + "/chunk_" + str(i)
    mkdir(download_chunk_dir)
    for filename in filename_per_directory[i]:
        num_files_awaiting_processing += 1
        move(path.join(args.common_crawl_download_dir, filename), path.join(download_chunk_dir, filename))
    pipeline_output_dir = "ungoliant_pipeline_results/chunk_" + str(i)
    mkdir(pipeline_output_dir)
    ungoliant_pipeline_output_dirs.append(pipeline_output_dir)
    dirs_awaiting_processing.append({"pipeline_output_dir": pipeline_output_dir, "download_chunk_dir": download_chunk_dir})
    if num_files_awaiting_processing >= args.num_proc:
        do_parallel_pipeline_processing(dirs_awaiting_processing)
        num_files_awaiting_processing = 0
        dirs_awaiting_processing = []
do_parallel_pipeline_processing(dirs_awaiting_processing)

# For some reason, datasets errors out if we try to load directly from the jsonl, so we need to do this first
processes = []
for ungoliant_pipeline_output_dir in ungoliant_pipeline_output_dirs:
    def convert_to_parquet(ungoliant_pipeline_output_dir):
        i = 0
        print("Chunking the ungoliant json into several parquet files before loading into huggingface dataset.")
        parquet_file_dir = path.join(ungoliant_pipeline_output_dir, "en_parquet")
        mkdir(parquet_file_dir)
        for chunk in tqdm(pd.read_json(path.join(ungoliant_pipeline_output_dir, "en_meta.jsonl"), lines=True, chunksize=10000)):
            parquet_file_path = path.join(parquet_file_dir, str(i) + ".parquet")
            chunk.to_parquet(parquet_file_path)
            i += 1
    p = Process(target=convert_to_parquet, args=(ungoliant_pipeline_output_dir,))
    p.start()
    processes.append(p)

for p in processes:
    p.join()

ds = Dataset.from_parquet([path.join(ungoliant_pipeline_output_dir, "en_parquet", "*.parquet") for ungoliant_pipeline_output_dir in ungoliant_pipeline_output_dirs])
ds = ds.map(lambda example, index: {"id": index, "text": example["content"], "url": example["warc_headers"]["warc-target-uri"], "timestamp": dateutil.parser.parse(example["warc_headers"]["warc-date"]).timestamp()}, num_proc=args.num_proc, with_indices=True, remove_columns=["content", "warc_headers", "metadata"])
ds.push_to_hub(args.output_dataset_name)
shutil.rmtree("ungoliant_pipeline_results")
shutil.rmtree("errors.txt")
