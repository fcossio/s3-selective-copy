import os
from multiprocessing import Pool
from typing import Generator, Iterable, List
from urllib.parse import urlparse

import boto3
from jsonargparse import CLI


def batcher(iterable: Iterable, batch_size: int) -> Generator[List, None, None]:
    """Batch an iterator. The last item might be of smaller len than batch_size.

    Args:
        iterable (Iterable): Any iterable that should be batched
        batch_size (int): Len of the generated lists

    Yields:
        Generator[List, None, None]: List of items in iterable
    """
    batch = []
    counter = 0
    for i in iterable:
        batch.append(i)
        counter += 1
        if counter % batch_size == 0:
            yield batch
            batch = []
    if len(batch) > 0:
        yield batch


def download_batch(batch):
    s3 = boto3.client("s3")
    n = 0
    for line in batch:
        dst, line = line
        url = urlparse(line)
        url_path = url.path.lstrip("/")
        folder, basename = os.path.split(url_path)
        dir = os.path.join(dst, folder)
        os.makedirs(dir, exist_ok=True)
        filepath = os.path.join(dir, basename)
        print(f"{filepath}")
        s3.download_file(url.netloc, url_path, filepath)
        n += 1
    return n


def file_reader(fp, dst):
    with open(fp) as f:
        for line in f:
            line = line.rstrip("\n")
            yield dst, line


def copy_cli(txt_path: str, dst: str = os.getcwd(), n_cpus: int = os.cpu_count()):
    """Copy files from s3 based on a list of urls. The output folder structure follows
    the s3 path.

    Args:
        txt_path (str): path to your list of files. One url per line.
        dst (str): path to store the files.
        n_cpus (int): number of simultaneous batches. Defaults to the number of cpus in
         the computer.
    """
    total_files = sum([1 for _ in file_reader(txt_path, dst)])
    print(n_cpus)
    n_cpus = min(total_files, n_cpus)
    batch_size = total_files // n_cpus
    with Pool(processes=n_cpus) as pool:
        for n in pool.imap_unordered(
            download_batch, batcher(file_reader(txt_path, dst), batch_size)
        ):
            pass


if __name__ == "__main__":
    CLI(copy_cli)
