# Follow these steps to create your very own Common Crawl pretraining dataset

## One time only
`bash download_pipeline_processing_models.sh`

## Every time

Use the following commands to get a dataset. They should take only a few min if you have lots of CPUs.

```
python download_common_crawl.py --snapshots CC-MAIN-2022-33 --segment_sampling_ratios 0.0001 --download_dir=common_crawl_wet_downloads --num_proc=96
python get_dataset_from_downloads.py --download_dir=common_crawl_wet_downloads --output_dataset_name=cc_raw --num_proc=96
python remove_wikipedia_urls.py --input_dataset_name=cc_raw --output_dataset_name=cc_no_wikipedia --url_column=url --split=en --num_proc=96
python apply_bigscience_filters.py --input_dataset_name=cc_no_wikipedia --output_dataset_name=cc_filtered --lang_id=en --text_column=text --num_proc=96
ulimit -Sn 1000000 && python deduplicate.py --input_dataset_name=cc_filtered --output_dataset_name=cc_olm --text_column=text --num_proc=96
```

You can then upload the final dataset to the Hugging Face Hub from a Python terminal like this:

```
from datasets import load_from_disk

ds = load_from_disk("cc_olm")

ds = ds.shuffle()  # Optionally, shuffle the dataset so you can get an idea of what a random sample of the dataset looks like in the Hugging Face Hub dataset preview.

ds.push_to_hub("cc_olm")
```

Increase `--segment_sampling_ratios` to get a larger dataset (it goes up to `1`). In the above code, `0.0001` means that it only uses a sample of `0.01%` of the data from a Common Crawl snapshot. To generate a dataset for the Online Language Modelling Project, we are currently using a 20% random sample of the latest common crawl snapshot. Note that there are so many duplicates in Common Crawl data, that the deduplication script needs about 800GB of RAM when you specify `--segment_sampling_ratios 0.20`.

# Documentation

Run any of the python commands with the `--help` flag. For example, `python download_common_crawl.py --help`.
