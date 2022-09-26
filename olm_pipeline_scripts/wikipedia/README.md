Follow these steps to download, clean, and push your very own Wikipedia pretraining dataset to the Hugging Face hub, replacing the arguments to suit your needs:
1. `python process_wikipedia_and_push_to_hub.py --wikipedia_snapshot=20220920 --output_dataset_name=Tristan/olm-wikipedia`

You can get the names for the latest wikipedia snapshots here: [https://dumps.wikimedia.org/enwiki/](https://dumps.wikimedia.org/enwiki/).

Unlike the Common Crawl code, this Wikipedia code is unfortunately not written for multiple CPUs. It will likely a bit over 30 hours :(. Also you need to make sure you have enough RAM. We needed 100's G's of RAM.

