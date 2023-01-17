# Online Language Modelling Dataset Pipeline

This repo enables you to pull a large and up-to-date text corpus from the web. It uses state-of-the-art processing methods to produce a clean text dataset that you can immediately use to pretrain a large language model, like BERT, GPT, or BLOOM. The main use-case for this repo is the Online Language Modelling Project, where we want to keep a language model up-to-date by pretraining it on the latest Common Crawl and Wikipedia dumps every month or so. You can see the models for the OLM project here: https://huggingface.co/olm. They actually get better performance than their original static counterparts.

Specifically, this repo has modular Python commands that enable you to:
* Specify Common Crawl web snapshots, or just Wikipedia snapshots. Then pull the data.
* Filter the data for a particular language, like English or French.
* Run the OSCAR filters used by BigScience for the BLOOM language model. These filters ensure some level of text quality and reduce pornographic content.
* Deduplicate the data.

This code is also fairly parallelized, although it can certianly be improved further. It can process over a terabyte from Common Crawl in a day or two, and all of English Wikipedia in less than an hour if you have:
* A machine with a lot of CPUs and memory.
* A fast internet connection.

## Setup
1. If you want to use this repo to generate a decent amount of data, get a machine with lots of CPUs and memory. We use an `n2d-standard-224` running `Ubuntu 20.04 LTS` on GCP. Add Terabytes of disk space too. You may need an even larger machine if you want to process close to 100% of a Common Crawl snapshot or several snapshots, particularly due to how much memory the deduplication process uses.
2. Clone with submodules: `git clone --recursive git@github.com:huggingface/olm-datasets.git`
3. Install cargo (rust package manager) with `curl https://sh.rustup.rs -sSf | sh`. Then install Ungoliant with `cargo install ungoliant@1.2.3`. You may need to install gcc and cmake first.
4. Set up a Python 3.9 environment, and run `pip install -r requirements.txt`
5. Run `huggingface-cli login`. This cli should have been installed from `requirements.txt`. To login, you need to paste a token from your account at [https://huggingface.co](https://huggingface.co). This step is necessary for the pipeline to push the generated datasets to your Hugging Face account.

## Getting a clean and up-to-date Common Crawl corpus

Follow the instructions at [pipeline_scripts/common_crawl](pipeline_scripts/common_crawl).

Here is the output dataset to expect from a 20% random segment sample of the August 2022 Common Crawl Snapshot: [https://huggingface.co/datasets/Tristan/olm-CC-MAIN-2022-33-sampling-ratio-0.20](https://huggingface.co/datasets/Tristan/olm-CC-MAIN-2022-33-sampling-ratio-0.20)

## Getting a clean and up-to-date Wikipedia corpus

Follow the instructions at [pipeline_scripts/wikipedia](pipeline_scripts/wikipedia).

Here is the output dataset to expect from a September 2022 snapshot of Wikipedia: [https://huggingface.co/datasets/Tristan/olm-wikipedia-20220920](https://huggingface.co/datasets/Tristan/olm-wikipedia-20220920)

## Analyzing the corpora

Follow the instructions at [analysis_scripts](analysis_scripts).

Here is a tweet thread which utilizes these scripts: [https://twitter.com/TristanThrush/status/1582356055794733057](https://twitter.com/TristanThrush/status/1582356055794733057)

Here is another tweet thread that dives a little deeper:
[https://twitter.com/TristanThrush/status/1588156731909029889](https://twitter.com/TristanThrush/status/1588156731909029889)

And here is a colab where you can quickly run some of the analysis yourself! [https://colab.research.google.com/drive/18Wv7ghW2rRjEe3oWDqh2iz9qqO8O6XcX?usp=sharing](https://colab.research.google.com/drive/18Wv7ghW2rRjEe3oWDqh2iz9qqO8O6XcX?usp=sharing)
