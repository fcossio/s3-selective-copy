# s3-selective-copy
Copy a subset of s3 files based on a txt of urls using multiprocessing.

# Usage
```bash
pip install requirements.txt
```

`my_list.txt`
```
s3://path/to/file1.abc
s3://path/to/file2.cdf
```


```bash
python s3co.py my_list.txt --dst ../my_dst_path/ --n_cpus=5
```