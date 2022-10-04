Per the repository [here](https://huggingface.co/datasets/Tristan/wikipedia), all you need is this Python code:

```
from datasets import load_dataset

ds = load_dataset("Tristan/wikipedia", language="en", date="20220920")

ds.save_to_disk("wikipedia_en_20220920")
ds.push_to_hub("wikipedia_en_20220920")
````

The code pulls the Wikipedia snapshot for the given date and language and does all the processing required to turn it into a clean pretraining dataset.

You can get the dates for the latest wikipedia snapshots here: [https://dumps.wikimedia.org/enwiki/](https://dumps.wikimedia.org/enwiki/).

It should take a bit under an hour on a GCP `n1-standard-96`.

