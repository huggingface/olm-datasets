from os import mkdir
from subprocess import run
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--common_crawl_snapshots", nargs='+')
parser.add_argument("--common_crawl_snapshot_offsets", type=int, nargs="+")
parser.add_argument("--download_dir")
parser.add_argument("--num_proc", type=int)
args = parser.parse_args()

run(f"mkdir {args.download_dir}", shell=True)
for index in range(len(args.common_crawl_snapshots)):
    # Download the data for a certian common crawl snapshot
    download_dir_name = f"ungoliant_downloads-{args.common_crawl_snapshots[index]}"
    run(f"mkdir {download_dir_name}", shell=True)
    run(f"wget https://data.commoncrawl.org/crawl-data/{args.common_crawl_snapshots[index]}/wet.paths.gz", shell=True)
    run("gzip -d wet.paths.gz", shell=True)
    wet_paths_name = f"wet-{args.common_crawl_snapshots[index]}.paths"
    run(f"mv wet.paths {wet_paths_name}", shell=True)
    run(f"ungoliant download -t={args.num_proc} -o={args.common_crawl_snapshot_offsets[index]} {wet_paths_name} {download_dir_name}", shell=True)
    run(f"rm {wet_paths_name}", shell=True)

    # Now, add 0's to the filename for every downloaded file. We want the number of 0's to be different than those from another common crawl snapshot
    # because we want every file to have a unique name accross multiple snapshot downloads.
    run(f"cd {download_dir_name} && for f in * ; do mv \"$f\" {'0'*index}\"$f\" ; done", shell=True)

    # Now we can move the downloaded files into the main download dir which has the downloads from the rest of this for loop.
    run("mv {download_dir_name}/* {args.download_dir}/", shell=True)
    run("rm -r {download_dir_name}", shell=True)

