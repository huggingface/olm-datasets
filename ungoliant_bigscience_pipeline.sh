# exit when any command fails
set -e

crawl=$1
num_proc=$2
offset=$3
huggingface_username=$4

mkdir ungoliant_downloads
mkdir ungoliant_pipeline_results
wget https://data.commoncrawl.org/crawl-data/$crawl/wet.paths.gz
gzip -d wet.paths.gz
ungoliant download -t=$num_proc -o=$offset wet.paths ungoliant_downloads
rm -r wet.paths
ungoliant pipeline --lid-path=sp_kenlm_ft_models/lid.176.bin ungoliant_downloads ungoliant_pipeline_results
rm -r ungoliant_downloads
mkdir ungoliant_pipeline_results_en_parquet
python remove_wikipedia_and_push_ungoliant_raw_to_hub.py $crawl-offset-$offset $num_proc $huggingface_username
rm -r ungoliant_pipeline_results
rm -r ungoliant_pipeline_results_en_parquet
python data-preparation/preprocessing/training/01b_oscar_cleaning_and_filtering/main_filtering.py --dataset_name=$huggingface_username/ungoliant-for-olm-raw-$crawl-offset-$offset --config_name=Tristan--ungoliant-for-olm-raw-$crawl --lang_dataset_id=en --path_fasttext_model=sp_kenlm_ft_models/lid.176.bin --path_sentencepiece_model=sp_kenlm_ft_models/en.sp.model --path_kenlm_model=sp_kenlm_ft_models/en.arpa.bin --path_dir_save_dataset=ungoliant_filtered
python dedup_ungoliant_filtered_and_push_to_hub.py $crawl-offset-$offset $num_proc $huggingface_username
rm -r cache
rm -r ungoliant_filtered
