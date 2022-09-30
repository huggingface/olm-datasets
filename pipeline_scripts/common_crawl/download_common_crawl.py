from os import mkdir
from subprocess import run
import argparse
import random
random.seed(42)

parser = argparse.ArgumentParser()
parser.add_argument("--snapshots", nargs='+')
parser.add_argument("--download_dir")
parser.add_argument("--segment_sampling_ratios", type=float, nargs="+")
parser.add_argument("--num_proc", type=int)
args = parser.parse_args()

run(f"mkdir {args.download_dir}", shell=True)
for index in range(len(args.snapshots)):
    # Download the data for a certian common crawl snapshot
    tmp_download_dir_name = f"ungoliant_downloads-{args.snapshots[index]}"
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

run("rm -r errors.txt", shell=True)

