# Follow these steps to analyze the Common Crawl or Wikipedia pretraining datasets that you created.

## To analyze for date mentions in the text

`python date_analysis.py --input_dataset_name=Tristan/olm-wikipedia20220920 --text_column=text --split=train --num_proc=96 --load_from_hub_instead_of_disk`
