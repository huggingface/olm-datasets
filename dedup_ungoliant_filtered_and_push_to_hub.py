from datasets import load_from_disk
from text_dedup.embedders.suffix import SuffixArrayEmbedder
import sys

dset_suffix = sys.argv[1]
num_proc = int(sys.argv[2])
huggingface_username = sys.argv[3]

d = load_from_disk("./ungoliant_filtered/en")

# If an example in our courpus has a byte string of 100 or longer which is duplicated elsewhere in the corpus, remove the example.
embedder = SuffixArrayEmbedder(k=100)
slices = embedder.embed_bash(d["text"])
d = d.filter(lambda example, idx: slices[idx] == [], num_proc=num_proc, with_indices=True)

d.push_to_hub(huggingface_username + "/ungoliant-for-olm-deduped-and-content-filtered-" + dset_suffix)
        
