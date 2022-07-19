import time
import os
import shutil
import sys
import pickle
from pathlib import Path
from util.heapq_adapt import merge


class BigList:
    def __init__(self, root, max_length: int = 2):
        self.root = root + f"_{hash(time.time())}"
        Path(self.root).mkdir(parents=True, exist_ok=True)

        self.max_length = max_length
        self.set = set()
        self.file_count = 0
        self.key = None
        self.cached_file = None
        self.cache = None
        self.idx = -1
        self.len = None

    def __next__(self):
        self.idx += 1
        if self.idx >= self.len-1:
            self.idx = -1
            raise StopIteration
        else:
            return self[self.idx]

    def __iter__(self):
        return self

    def __del__(self):
        shutil.rmtree(self.root)

    def __getitem__(self, indices):
        # Todo remember last indices and load form cash
        idx_file = indices // self.max_length
        idx_elem = indices % self.max_length
        self.save_set()
        if idx_file != self.cached_file:
            self.cache = sorted(list(pickle.load(open(f"{self.root}/{idx_file}.pkl", "rb"))), key=self.key)
            self.cached_file = idx_file
        return self.cache[idx_elem]

    def __len__(self):
        # Todo optimize this
        self.save_set()
        lenght = 0
        for i in range(self.file_count):
            lenght += len(pickle.load(open(f"{self.root}/{i}.pkl", "rb")))

        return lenght

    def set_len(self):
        self.len = len(self)

    def add(self, element):
        self.set.add(element)
        if len(self.set) > self.max_length:
            self.save_set()

    def save_set(self):
        # Todo balances, that indexing still is true
        if len(self.set) > 0:
            with open(f"{self.root}/{self.file_count}.pkl", "wb") as f:
                pickle.dump(self.set, f)
                self.set.clear()
                self.file_count += 1

    def set_key(self, key):
        self.cached_file = None
        self.key = key

    def sort(self, reverse: bool = False):
        self.cached_file = None
        self.save_set()

        chunks = [open(f"{self.root}/{c}.pkl", "rb") for c in range(self.file_count)]
        old_root = self.root
        self.root = f"{self.root}_{hash(time.time())}"
        Path(self.root).mkdir(parents=True, exist_ok=True)
        self.file_count = 0

        for elem in merge(*chunks, key=self.key, reverse=reverse):
            self.add(elem)

        shutil.rmtree(old_root)


if __name__ == '__main__':
    a = BigList(root="tmp", max_length=10000)

    for i in range(int(1e5)):
        a.add((-1 * i, -1 * i))

    a.set_key(lambda tup: tup[-1])
    a.sort()

    for i in range(10):
        print(a[i])
