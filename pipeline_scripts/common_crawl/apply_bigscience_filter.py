from datasets import load_dataset, load_from_disk
import argparse
from shutil import rmtree
import sys
import uuid

sys.path.append("data-preparation/preprocessing/training/01b_oscar_cleaning_and_filtering")
from filtering import DatasetFiltering

parser = argparse.ArgumentParser()
parser.add_argument("--input_dataset_name")
parser.add_argument("--output_dataset_name")
parser.add_argument("--lang_id")
parser.add_argument("--text_column_name")
parser.add_argument("--num_proc", type=int)
parser.add_argument("--push_to_hub", action="store_true")
args = parser.parse_args()

ds = load_dataset(args.input_dataset_name)

# We have to do this if the text column is not named "text" in the dataset,
# because DatasetFiltering assumes that the name is "text".
temp_column_name = None
if args.text_column != "text":
    if "text" in ds.colum_names:
        temp_column_name = str(uuid.uuid4())
        ds = ds.rename_column("text", temp_column_name)
    ds = ds.rename_column(args.text_column, "text")

dataset_filtering = DatasetFiltering(
    dataset=ds,
    lang_dataset_id=args.lang_id,
    path_fasttext_model="sp_kenlm_ft_models/lid.176.bin",
    path_sentencepiece_model=f"sp_kenlm_ft_models/{args.lang_id}.sp.model",
    path_kenlm_model=f"sp_kenlm_ft_models/{args.lang_id}.arpa.bin",
    num_proc=args.num_proc,
    path_dir_save_dataset="tmp_dataset",
)

dataset_filtering.modifying_documents()
dataset_filtering.filtering()
dataset_filtering.save_dataset()

ds = load_from_disk("tmp_dataset/" + args.lang_id)

# We have to do this if the text column is not named "text" in the dataset,
# because DatasetFiltering assumes that the name is "text".
if args.text_column != "text":
    ds = ds.rename_column("text", args.text_column)
    if temp_column_name is not None:
        ds = ds.rename_column(temp_column_name, "text")

ds.save_to_disk(args.output_dataset_name)
rmtree("tmp_dataset")

if args.push_to_hub:
    ds.push_to_hub(args.output_dataset_name)
