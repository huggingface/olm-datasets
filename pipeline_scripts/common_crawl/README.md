# Quick start

This section provides all the commands that you need to generate a deduplicated and filtered dataset from Common Crawl, ready for pretraining!

## One time only

`bash download_pipeline_processing_models.sh`

## Every time

Use the following commands to get a dataset. They should take only a few min if you have lots of CPUs. Adjust `--num_proc` to be equal to however many CPUs that you have.

```
python download_common_crawl.py --snapshots CC-MAIN-2022-33 --segment_sampling_ratios 0.0001 --download_dir=common_crawl_wet_downloads --num_proc=224
python get_dataset_from_downloads.py --download_dir=common_crawl_wet_downloads --output_dataset_name=cc_raw --num_proc=224
python remove_wikipedia_urls.py --input_dataset_name=cc_raw --output_dataset_name=cc_no_wikipedia --url_column=url --split=en --num_proc=224
python apply_bigscience_filters.py --input_dataset_name=cc_no_wikipedia --output_dataset_name=cc_filtered --lang_id=en --text_column=text --num_proc=224
ulimit -Sn 1000000 && python deduplicate.py --input_dataset_name=cc_filtered --output_dataset_name=cc_olm --text_column=text --num_proc=224
```

You can then upload the final dataset to the Hugging Face Hub from a Python terminal like this:

```
from datasets import load_from_disk

ds = load_from_disk("cc_olm")

ds = ds.shuffle()  # Optionally, shuffle the dataset so you can get an idea of what a random sample of the dataset looks like in the Hugging Face Hub dataset preview.

ds.push_to_hub("cc_olm")
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

## Why do we specify a 30 gigabyte Common Crawl dataset for the OLM project?

When combined with a Wikipedia snapshot, this level of data is about 8 to 9 billion tokens. So, it is compute-optimal for training a model at the scale of RoBERTa-large according to the [Chinchilla Paper](https://arxiv.org/abs/2203.15556).

## Limitations of the deduplication code

There are tons of duplicates in Common Crawl data, which means that the deduplication script will need about 700 to 900 gigabytes of RAM if you want to generate a 30 gigabyte dataset like us :(. The main problem is this issue in the Google deduplication code: [https://github.com/google-research/deduplicate-text-datasets/issues/18](https://github.com/google-research/deduplicate-text-datasets/issues/18).


# More documentation

Run any of the python commands with the `--help` flag. For example, `python download_common_crawl.py --help`.
