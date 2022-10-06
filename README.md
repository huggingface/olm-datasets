# Online Language Modelling Dataset Pipeline (🚧 WIP 🚧)

This repo enables you to pull a large and up-to-date text corpus from the web. It uses state-of-the-art processing methods to produce a clean text dataset that you can immediately use to pretrain a large language model, like BERT, GPT, or BLOOM.

Specifically, this repo has modular Python commands that enable you to:
* Specify Common Crawl web snapshots, or just Wikipedia snapshots. Then pull the data.
* Filter the data for a particular language, like English or French.
* Run the OSCAR filters used by BigScience for the BLOOM language model. These filters ensure some level of text quality and reduce pornographic content.
* Deduplicate the data.

This code is also highly parallelized, although it can certianly be improved further. It can process over a terabyte from Common Crawl in about a day, and all of English Wikipedia in less than an hour if you have:
* A machine with a lot of CPUs and memory.
* fast internet connection.

## Setup
1. If you want to use this repo to generate a decent amount of data, get a machine with lots of CPUs and memory. We use an `n2d-standard-224` running `Ubuntu 20.04 LTS` on GCP. Add Terabytes of disk space too. You may need an even larger machine if you want to process close to 100% of a Common Crawl snapshot or several snapshots, particularly due to how much memory the deduplication process uses.
2. Clone with submodules: `git clone --recursive git@github.com:huggingface/olm-datasets.git`
3. Install cargo (rust package manager) with `curl https://sh.rustup.rs -sSf | sh`. Then install Ungoliant with `cargo install ungoliant@1.2.3`. You may need to install gcc and cmake first.
4. Set up a Python 3.9 environment, and run `pip install -r requirements.txt`
5. Run `huggingface-cli login`. This cli should have been installed in the requirements.txt. To login, you need to paste a token from your account at [https://huggingface.co](https://huggingface.co). This step is necessary for the pipeline to push the generated datasets to your Hugging Face account.

## Getting a clean and up-to-date Common Crawl corpus

Follow the instructions at [pipeline_scripts/common_crawl](pipeline_scripts/common_crawl).

## Getting a clean and up-to-date Wikipedia corpus

Follow the instructions at [pipeline_scripts/wikipedia](pipeline_scripts/wikipedia).

## Analyzing the corpora

Follow the instructions at [analysis_scripts](analysis_scripts).
