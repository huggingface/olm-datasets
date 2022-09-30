# Follow these steps to create your very own Common Crawl pretraining dataset

## One time only
`bash download_pipeline_processing_models.sh`

## Every time

Use the following commands to get a small sample of the final processed dataset. They should take only a few min on an `n1-standard-96`.

```
python download_common_crawl.py --snapshots CC-MAIN-2022-33 --segment_sampling_ratios 0.0001 --download_dir=common_crawl_wet_downloads --num_proc=96
python get_dataset_from_downloads.py --download_dir=common_crawl_wet_downloads --output_dataset_name=cc_raw --num_proc=96
python remove_wikipedia_urls.py --input_dataset_name=cc_raw --output_dataset_name=cc_no_wikipedia --url_column=url --split=en --num_proc=96
python apply_bigscience_filters.py --input_dataset_name=cc_no_wikipedia --output_dataset_name=cc_filtered --lang_id=en --text_column=text --num_proc=96
ulimit -Sn 1000000 && python deduplicate.py --input_dataset_name=cc_filtered --output_dataset_name=cc_olm --text_column=text --url_column=url --timestamp_column=timestamp --num_proc=96
```

You can then upload the final dataset to the huggingface hub from a Python terminal like this:

```
from datasets import load_from_disk

ds = load_from_disk("cc_olm")

ds = ds.shuffle()  # Optionally, shuffle the dataset so you can get an idea of what a random sample of the dataset looks like in the Hugging Face Hub dataset preview.

ds.push_to_hub("cc_olm")
```

Some notes:

* To get more data, you can increase `segment_sampling_ratios` all the way up to `1`.
* Each of the python commands (besides `download_common_crawl.py`) saves a Hugging Face dataset that the next python command consumes. You can push these intermediate datasets to the Hugging Face Hub too, and load them to take a look.

