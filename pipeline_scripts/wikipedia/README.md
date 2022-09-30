Per the repository [here](https://huggingface.co/datasets/wikipedia), all you need is this Python code:

```
from datasets import load_dataset

ds = load_dataset("wikipedia", language="en", date="20220920", save_infos=True, beam_runner="DirectRunner")

ds.save_to_disk("wikipedia_en_20220920")
````

You can get the names for the latest wikipedia snapshots here: [https://dumps.wikimedia.org/enwiki/](https://dumps.wikimedia.org/enwiki/).

Unlike the Common Crawl code, this Wikipedia code is unfortunately not written for multiple CPUs. It will likely take a bit over 30 hours :(. Also you need to make sure you have enough RAM. We needed 100's of gigabytes of RAM.

