from datasets import load_dataset, load_from_disk
import argparse
from shutil import rmtree
import sys

sys.path.append("data-preparation/preprocessing/training/01b_oscar_cleaning_and_filtering")
from filtering import DatasetFiltering

# TODO (Tristan): This assumes that the name of the text_column is "text". Make it configurable.
parser = argparse.ArgumentParser()
parser.add_argument("--input_dataset_name")
parser.add_argument("--output_dataset_name")
parser.add_argument("--num_proc", type=int)
args = parser.parse_args()

ds = load_dataset(args.input_dataset_name)

dataset_filtering = DatasetFiltering(
    dataset=ds,
    lang_dataset_id="en",
    path_fasttext_model="sp_kenlm_ft_models/lid.176.bin",
    path_sentencepiece_model="sp_kenlm_ft_models/en.sp.model",
    path_kenlm_model="sp_kenlm_ft_models/en.arpa.bin",
    num_proc=args.num_proc,
    path_dir_save_dataset="tmp_dataset",
)

dataset_filtering.modifying_documents()
dataset_filtering.filtering()
dataset_filtering.save_dataset()

ds = load_from_disk("tmp_dataset/en")
ds.push_to_hub(args.output_dataset_name)
rmtree("tmp_dataset")
