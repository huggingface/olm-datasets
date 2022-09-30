from datasets import load_dataset
from tqdm import tqdm
import pandas as pd
import subprocess
from multiprocessing import Process
from os import walk, mkdir, path
from shutil import move, rmtree
import dateutil
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--download_dir")
parser.add_argument("--output_dataset_name")
parser.add_argument("--num_proc", type=int)
parser.add_argument("--push_to_hub", action="store_true")
args = parser.parse_args()

filenames = next(walk(args.download_dir), (None, None, []))[2]

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
    download_chunk_dir = path.join(args.download_dir, "chunk_" + str(i))
    mkdir(download_chunk_dir)
    for filename in filename_per_directory[i]:
        num_files_awaiting_processing += 1
        move(path.join(args.download_dir, filename), path.join(download_chunk_dir, filename))
    pipeline_output_dir = path.join("ungoliant_pipeline_results", "chunk_" + str(i))
    mkdir(pipeline_output_dir)
    ungoliant_pipeline_output_dirs.append(pipeline_output_dir)
    dirs_awaiting_processing.append({"pipeline_output_dir": pipeline_output_dir, "download_chunk_dir": download_chunk_dir})
    if num_files_awaiting_processing >= args.num_proc:
        do_parallel_pipeline_processing(dirs_awaiting_processing)
        num_files_awaiting_processing = 0
        dirs_awaiting_processing = []

do_parallel_pipeline_processing(dirs_awaiting_processing)

for i in range(len(filename_per_directory)):
    download_chunk_dir = path.join(args.download_dir, "chunk_" + str(i))
    for filename in filename_per_directory[i]:
        move(path.join(download_chunk_dir, filename), path.join(args.download_dir, filename))
    rmtree(download_chunk_dir)

# For some reason, datasets errors out if we try to load directly from the jsonl, so we need to do this first
processes = []
for ungoliant_pipeline_output_dir in ungoliant_pipeline_output_dirs:
    language_filenames = [name for name in next(walk(ungoliant_pipeline_output_dir), (None, None, []))[2] if name.endswith("_meta.jsonl")]
    language_ids = [language_filename.split("_")[0] for language_filename in language_filenames]
    def convert_to_parquet_and_reformat(ungoliant_pipeline_output_dir):
        for language_filename in language_filenames:
            language_id = language_filename.split("_")[0]
            i = 0
            print("Chunking the ungoliant json into several parquet files before loading into huggingface dataset.")
            parquet_file_dir = path.join(ungoliant_pipeline_output_dir, language_id + "_parquet")
            mkdir(parquet_file_dir)
            for chunk in tqdm(pd.read_json(path.join(ungoliant_pipeline_output_dir, language_id + "_meta.jsonl"), lines=True, chunksize=10000)):
                parquet_file_path = path.join(parquet_file_dir, str(i) + ".parquet")
                chunk["url"] = chunk.apply(lambda row: row["warc_headers"]["warc-target-uri"], axis=1)
                chunk["timestamp"] = chunk.apply(lambda row: dateutil.parser.parse(row["warc_headers"]["warc-date"]).timestamp(), axis=1)
                chunk.drop(columns=["warc_headers", "metadata"], inplace=True)
                chunk.rename(columns={"content": "text"}, inplace=True)
                chunk.to_parquet(parquet_file_path)
                i += 1
    p = Process(target=convert_to_parquet_and_reformat, args=(ungoliant_pipeline_output_dir,))
    p.start()
    processes.append(p)

for p in processes:
    p.join()

data_files = {language_id: [path.join(ungoliant_pipeline_output_dir, language_id + "_parquet", "*.parquet") for ungoliant_pipeline_output_dir in ungoliant_pipeline_output_dirs] for language_id in language_ids}
ds = load_dataset("parquet", data_files=data_files)
ds.save_to_disk(args.output_dataset_name)
rmtree("ungoliant_pipeline_results")

if args.push_to_hub:
    ds.push_to_hub(args.output_dataset_name)
