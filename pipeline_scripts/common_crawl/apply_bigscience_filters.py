from datasets import load_dataset, load_from_disk
import argparse
from os import path, mkdir
from shutil import rmtree
import sys
import uuid

sys.path.append("data-preparation/preprocessing/training/01b_oscar_cleaning_and_filtering")
from filtering import DatasetFiltering

parser = argparse.ArgumentParser()
parser.add_argument("--input_dataset_name")
parser.add_argument("--output_dataset_name")
parser.add_argument("--lang_id")
parser.add_argument("--split", default=None)
parser.add_argument("--text_column")
parser.add_argument("--num_proc", type=int)
parser.add_argument("--push_to_hub", action="store_true")
parser.add_argument("--tmp_dir", default=".tmp_apply_bigscience_filters")
parser.add_argument("--load_from_hub_instead_of_disk", action="store_true")
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

# We have to do this if the text column is not named "text" in the dataset,
# because DatasetFiltering assumes that the name is "text".
temp_column_name = None
if args.text_column != "text":
    if "text" in ds.colum_names:
        temp_column_name = str(uuid.uuid4())
        ds = ds.rename_column("text", temp_column_name)
    ds = ds.rename_column(args.text_column, "text")

if path.exists(args.tmp_dir):
    run(f"rm -r {args.tmp_dir}", shell=True)

mkdir(args.tmp_dir)
tmp_dataset_name = path.join(args.tmp_dir, "intermediate_bigscience_filtered_dataset")

dataset_filtering = DatasetFiltering(
    dataset=ds,
    lang_dataset_id=args.lang_id,
    path_fasttext_model="sp_kenlm_ft_models/lid.176.bin",
    path_sentencepiece_model=f"sp_kenlm_ft_models/{args.lang_id}.sp.model",
    path_kenlm_model=f"sp_kenlm_ft_models/{args.lang_id}.arpa.bin",
    num_proc=args.num_proc,
    path_dir_save_dataset=tmp_dataset_name,
)

dataset_filtering.modifying_documents()
dataset_filtering.filtering()
dataset_filtering.save_dataset()

ds = load_from_disk(path.join(tmp_dataset_name, args.lang_id))

# We have to do this if the text column is not named "text" in the dataset,
# because DatasetFiltering assumes that the name is "text".
if args.text_column != "text":
    ds = ds.rename_column("text", args.text_column)
    if temp_column_name is not None:
        ds = ds.rename_column(temp_column_name, "text")

ds.save_to_disk(args.output_dataset_name)
rmtree(args.tmp_dir)

if args.push_to_hub:
    ds.push_to_hub(args.output_dataset_name)
