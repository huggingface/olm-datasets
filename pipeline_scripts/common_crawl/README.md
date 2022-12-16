![olm_cc_pipeline](https://user-images.githubusercontent.com/20826878/199851707-64a7a026-c413-4d78-8b04-a825e07534b3.jpeg)

# Quick start

This section provides all the commands that you need to generate a deduplicated and filtered dataset from Common Crawl, ready for pretraining!

## One time only

`bash download_pipeline_processing_models.sh`

## Every time

Use the following commands to get a dataset. They should take only a few min if you have lots of CPUs. Adjust `--num_proc` to be equal to however many CPUs that you have.

```
python download_common_crawl.py --snapshots CC-MAIN-2022-33 --segment_sampling_ratios 0.0001 --seed=42 --download_dir=common_crawl_wet_downloads --num_proc=224
python get_text_dataset_from_wet_downloads.py --download_dir=common_crawl_wet_downloads --output_dataset_name=cc_raw --num_proc=224
python remove_wikipedia_urls.py --input_dataset_name=cc_raw --output_dataset_name=cc_no_wikipedia --url_column=url --split=en --num_proc=224
python apply_bigscience_filters.py --input_dataset_name=cc_no_wikipedia --output_dataset_name=cc_filtered --lang_id=en --text_column=text --num_proc=224
ulimit -Sn 1000000 && python deduplicate.py --input_dataset_name=cc_filtered --output_dataset_name=cc_olm --text_column=text --remove_whole_example --num_proc=224

# Optionally, get the last-modified headers from the websites and add them to the dataset. --segment_sampling_ratios and --seed must be the same as above for this to work.
python download_common_crawl.py --snapshots CC-MAIN-2022-33 --segment_sampling_ratios 0.0001 --seed=42 --download_dir=common_crawl_wat_downloads --paths_type=wat --num_proc=224
python get_last_modified_dataset_from_wat_downloads.py --download_dir=common_crawl_wat_downloads --output_dataset_name=cc_raw_last_modified --num_proc=224
python combine_last_modified_with_text_dataset.py --text_dataset_name=cc_olm --last_modified_dataset_name=cc_raw_last_modified --output_dataset_name=cc_olm_with_last_modified --url_column=url --crawl_timestamp_column=crawl_timestamp --last_modified_timestamp_column=last_modified_timestamp --num_proc=224

```

You can then upload the final dataset to the Hugging Face Hub from a Python terminal like this:

```
from datasets import load_from_disk

ds = load_from_disk("cc_olm")  # Or cc_olm_with_last_modified if you did the optional step above.

ds = ds.shuffle()  # Optionally, shuffle the dataset so you can get an idea of what a random sample of the dataset looks like in the Hugging Face Hub dataset preview.

ds.push_to_hub("cc_olm")  # Or cc_olm_with_last_modified if you did the optional step above.
```


# Important notes

## Finding the latest Common Crawl snapshots

They are displayed here: [https://commoncrawl.org/the-data/get-started/](https://commoncrawl.org/the-data/get-started/). Just enter the names of the snapshots you want as arguments to the `download_common_crawl.py` script.

## Intermediate dataset checkpoints

Each of the python scripts from the quick start commands saves a Hugging Face dataset to the disk. The dataset is then read by the next python command. These intermediate datasets are not deleted by default, so you can observe what each step of the pipeline does. This also means that you should have a large disk. We use a 15 terabyte disk for the Online Language Modelling Project.

## How to specify the size of the dataset

Increase `--segment_sampling_ratios` to get a larger dataset (it goes up to `1`). In the above quick start code, `0.0001` means that it only uses a sample of `0.01%` of the data from a Common Crawl snapshot. To generate a dataset for the Online Language Modelling Project, we are currently pulling about 1.45 terabytes from each Common Crawl snapshot, which is about 350 gigabytes after going through the BigScience filters and finally 30 gigabytes after going through the deduplication code. For the August 2022 snapshot, 1.45 terabytes is about 20% (i.e. `--segment_sampling_ratios 0.20`). Crawl sizes very though. For May 2022, 1.45 terabytes is about 14%.

If you want to train a larger model than us, then specify a higher value for `--segment_sampling_ratios`, or even use multiple Common Crawl snapshots like this:

```
python download_common_crawl.py --snapshots CC-MAIN-2022-27 CC-MAIN-2022-33 --segment_sampling_ratios 0.5 1 --download_dir=common_crawl_wet_downloads --num_proc=224
```

Keep in mind that, with more data, the deduplication script will need more RAM. Read on for limitations of the deduplication script.

## Limitations of the deduplication code

There are tons of duplicates in Common Crawl data, which means that the deduplication script will need 100's of gigabytes of RAM if you want to generate a 30 gigabyte dataset like us :(. If you want to get around this, there is also the option in the deduplication script for you to chunk the dataset and deduplicate each chunk individually. The main problem is this issue in the Google deduplication code: [https://github.com/google-research/deduplicate-text-datasets/issues/18](https://github.com/google-research/deduplicate-text-datasets/issues/18).


# More documentation

Run any of the python commands with the `--help` flag. For example, `python download_common_crawl.py --help`.
