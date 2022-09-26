# exit when any command fails
set -e

python data-preparation/preprocessing/training/01b_oscar_cleaning_and_filtering/download_sentencepiece_kenlm_models.py --output_dir_path=sp_kenlm_ft_models
wget https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin -P sp_kenlm_ft_models/
