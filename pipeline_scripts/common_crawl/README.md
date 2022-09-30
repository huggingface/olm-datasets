# Follow these steps to create your very own Common Crawl pretraining dataset

## One time only
`bash download_pipeline_processing_models.sh`

## Every time
```
python download_common_crawl.py --snapshots CC-MAIN-2022-33 --segment_sampling_ratios 0.02 --download_dir=common_crawl_wet_downloads --num_proc=96
python get_dataset_from_downloads.py --download_dir=common_crawl_wet_downloads --output_dataset_name=cc_raw --num_proc=96
python remove_wikipedia_urls.py --input_dataset_name=cc_raw --output_dataset_name=cc_no_wikipedia --url_column=url --split=en --num_proc=96
python apply_bigscience_filters.py --input_dataset_name=cc_no_wikipedia --output_dataset_name=cc_filtered --lang_id=en --text_column=text --num_proc=96
python deduplicate.py --input_dataset_name=cc_filtered --output_dataset_name=cc_olm --text_column=text --num_proc=96
```
