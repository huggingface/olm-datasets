Per the repository [here](https://huggingface.co/datasets/olm/wikipedia), just run this Python code. It uses all CPUs available and should take less than an hour if you have a lot of CPUs (on the order of 100).

```
from datasets import load_dataset

ds = load_dataset("olm/wikipedia", language="en", date="20220920")

ds.save_to_disk("wikipedia_en_20220920")
ds.push_to_hub("wikipedia_en_20220920")
````

The code pulls the Wikipedia snapshot for the given date and language and does all the processing required to turn it into a clean pretraining dataset. You can get the dates for the latest wikipedia snapshots here: [https://dumps.wikimedia.org/enwiki/](https://dumps.wikimedia.org/enwiki/).
