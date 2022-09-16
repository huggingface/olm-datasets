from datasets import load_dataset
import sys

date = sys.argv[1]
huggingface_username = sys.argv[2]

d = load_dataset("wikipedia", language="en", date=date, save_infos=True, beam_runner="DirectRunner")
d.push_to_hub(huggingface_username + "/wikipedia-for-olm-clean-" + language + "-" + date)
