---
language: 
  - es
  - af
  - ar
  - arz
  - as
  - bn
  - fr
  - sw
  - eu
  - ca
  - zh
  - en
  - hi
  - ur
  - id
  - pt
  - vi
  - gu
  - kn
  - ml
  - mr
  - ta
  - te
  - yo
tags:
- kenlm
- perplexity
- n-gram
- kneser-ney
- bigscience
license: "mit"
datasets:
- wikipedia
- oscar
---

# KenLM models
This repo contains several KenLM models trained on different tokenized datasets and languages.  
KenLM models are probabilistic n-gram languge models that models. One use case of these models consist on fast perplexity estimation for [filtering or sampling large datasets](https://huggingface.co/bertin-project/bertin-roberta-base-spanish). For example, one could use a KenLM model trained on French Wikipedia to run inference on a large dataset and filter out samples that are very unlike to appear on Wikipedia (high perplexity), or very simple non-informative sentences that could appear repeatedly (low perplexity).

At the root of this repo you will find different directories named after the dataset models were trained on (e.g. `wikipedia`, `oscar`). Within each directory, you will find several models trained on different language subsets of the dataset (e.g. `en (English)`, `es (Spanish)`, `fr (French)`). For each language you will find three different files
* `{language}.arpa.bin`: The trained KenLM model binary
* `{language}.sp.model`: The trained SentencePiece model used for tokenization
* `{language}.sp.vocab`: The vocabulary file for the SentencePiece model

The models have been trained using some of the preprocessing steps from [cc_net](https://github.com/facebookresearch/cc_net), in particular replacing numbers with zeros and normalizing punctuation. So, it is important to keep the default values for the parameters: `lower_case`, `remove_accents`, `normalize_numbers` and `punctuation` when using the pre-trained models in order to replicate the same pre-processing steps at inference time.

# Dependencies
* KenLM: `pip install https://github.com/kpu/kenlm/archive/master.zip`
* SentencePiece: `pip install sentencepiece`

# Example:
```
from model import KenlmModel


# Load model trained on English wikipedia
model = KenlmModel.from_pretrained("wikipedia", "en")

# Get perplexity
model.get_perplexity("I am very perplexed")
# 341.3 (low perplexity, since sentence style is formal and with no grammar mistakes)

model.get_perplexity("im hella trippin")
# 46793.5 (high perplexity, since the sentence is colloquial and contains grammar mistakes)
```
In the example above we see that, since Wikipedia is a collection of encyclopedic articles, a KenLM model trained on it will naturally give lower perplexity scores to sentences with formal language and no grammar mistakes than colloquial sentences with grammar mistakes.