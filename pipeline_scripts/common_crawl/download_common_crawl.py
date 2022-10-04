from os import mkdir, path
from subprocess import run
import argparse
import random
random.seed(42)

parser = argparse.ArgumentParser(description="Downloads raw Common Crawl WET files.")
parser.add_argument("--snapshots", nargs='+', help="The Common Crawl snapshots to download files from, such as CC-MAIN-2022-33 or CC-MAIN-2022-27. Several can be specified.", required=True)
parser.add_argument("--download_dir", help="The directory to download the data to.", required=True)
parser.add_argument("--segment_sampling_ratios", type=float, nargs="+", help="The ratios of each Common Crawl snapshot to use. The higher the ratio, the larger the generated dataset (but also the longer the time that the OLM pipeline runs). You should specify one for each snapshot. For example, if you specify '--snapshots CC-MAIN-2022-33 CC-MAIN-2022-27', then --segment_sampling_ratios could be '0.15 0.11'. This means that 15 percent of the segments from CC-MAIN-2022-33 will uniformly randomly sampled and used, and 11 percent of the segments from CC-MAIN-2022-27 will be uniformly randomly sampled and used.", required=True)
parser.add_argument("--tmp_dir", default=".tmp_download_common_crawl", help="The directory where temporary files are stored. They are deleted when this script completes. Default is .tmp_download_common_crawl.")
parser.add_argument("--num_proc", type=int, help="The number of processes to use.", required=True)
args = parser.parse_args()

if path.exists(args.download_dir):
    run(f"rm -r {args.download_dir}", shell=True)

if path.exists(args.tmp_dir):
    run(f"rm -r {args.tmp_dir}", shell=True)

run(f"mkdir {args.download_dir} {args.tmp_dir}", shell=True)
for index in range(len(args.snapshots)):
    # Download the data for a certian common crawl snapshot
    tmp_download_dir_name = f"{args.tmp_dir}/ungoliant_downloads-{args.snapshots[index]}"
    run(f"mkdir {tmp_download_dir_name}", shell=True)
    run(f"wget https://data.commoncrawl.org/crawl-data/{args.snapshots[index]}/wet.paths.gz", shell=True)
    run("gzip -d wet.paths.gz", shell=True)
    wet_paths_name = f"wet-{args.snapshots[index]}.paths"
    run(f"mv wet.paths {wet_paths_name}", shell=True)
    segments = open(wet_paths_name, "r").readlines()
    kept_segments = []
    for segment in segments:
        if random.random() <= args.segment_sampling_ratios[index]:
            kept_segments.append(segment)
    open(wet_paths_name, "w").writelines(kept_segments)
    run(f"ungoliant download -t={args.num_proc} {wet_paths_name} {tmp_download_dir_name}", shell=True)
    run(f"rm {wet_paths_name}", shell=True)

    # Now, add 0's to the filename for every downloaded file. We want the number of 0's to be different than those from another common crawl snapshot
    # because we want every file to have a unique name accross multiple snapshot downloads.
    if index > 0:
        run(f"cd {tmp_download_dir_name} && for f in * ; do mv \"$f\" {'0'*index}\"$f\" ; done", shell=True)

    # Now we can move the downloaded files into the main download dir which has the downloads from the rest of this for loop.
    run(f"mv {tmp_download_dir_name}/* {args.download_dir}/", shell=True)
    run(f"rm -r {tmp_download_dir_name}", shell=True)

run("rm -r {args.tmp_dir}", shell=True)
run("rm -r errors.txt", shell=True)

