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

Increase `--segment_sampling_ratios` to get a larger dataset (it goes up to `1`). In the above code, `0.0001` means that it only uses a sample of `0.01%` of the data from a Common Crawl snapshot. To generate a dataset for the Online Language Modelling Project, we are currently using a 20% random sample of the latest common crawl snapshot.


# Important notes

To get an OLM dataset from a Common Crawl snapshot, we currently use the commands from the quickstart, but with `--segment_sampling_ratios 0.20`

There are tons of duplicates in Common Crawl data, and our deduplication procedure is very aggressive. When you specify `--segment_sampling_ratios 0.20` to get the OLM dataset, keep in mind that the deduplication script will need about 800GB of RAM.

With 20% of a Common Crawl snapshot, the size of the dataset coming from the BigScience filters should be about 300GB when saved to your disk. After going through the deduplication step, it will only be about 30GB! When combined with a Wikipedia snapshot, this level of data is about compute-optimal for training a model at the scale of RoBERTa-large. But if you want to train a larger model, then specify a higher value for `--segment_sampling_ratios`, or even use multiple Common Crawl snapshots like this:

```
python download_common_crawl.py --snapshots CC-MAIN-2022-27 CC-MAIN-2022-33 --segment_sampling_ratios 0.5 1 --download_dir=common_crawl_wet_downloads --num_proc=224
```

Keep in mind that, with more data, the deduplication script will need even more RAM.


# More documentation

Run any of the python commands with the `--help` flag. For example, `python download_common_crawl.py --help`.
