# Online Language Modelling Dataset Pipeline

## Setup
1. Get a machine with lots of CPUs and memory. We use an n1-standard-96 Ubuntu 20.04 LTS machine on GCP. Add Terabytes of disk space too.
2. Install cargo (rust package manager) with `curl https://sh.rustup.rs -sSf | sh`. Then install Ungoliant with `cargo install ungoliant@1.2.3`. You may need to install gcc and cmake first.
3. Set up a Python 3.9 environment, and run `pip install -r requirements.txt`
5. Run `huggingface-cli login` (should have been installed in the requirements.txt) and then paste a token from your account at [https://huggingface.co](https://huggingface.co). This is necessary because the pipeline will push the finalized datasets to your Hugging Face account.

## Getting a clean and up-to-date Common Crawl corpus

Follow the instructions at [olm_pipeline_scripts/common_crawl](olm_pipeline_scripts/common_crawl).

## Getting a clean and up-to-date Wikipedia corpus

Follow the instructions at [olm_pipeline_scripts/wikipedia](olm_pipeline_scripts/wikipedia).
