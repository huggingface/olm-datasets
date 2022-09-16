# exit when any command fails
set -e

git clone https://github.com/bigscience-workshop/data-preparation.git
cd ./data-preparation
git checkout 6ee617239c09ea3533a12a80b432e061c34d6519
cd ../
python data-preparation/preprocessing/training/01b_oscar_cleaning_and_filtering/download_sentencepiece_kenlm_models.py --output_dir_path=sp_kenlm_ft_models
wget https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin -P sp_kenlm_ft_models/

