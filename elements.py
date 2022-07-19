import time
import os
import shutil
import sys
import pickle
from pathlib import Path
from util.heapq import merge


class BigList:
    def __init__(self, root, max_length: int = 2):
        self.root = root + f"_{hash(time.time())}"
        Path(self.root).mkdir(parents=True, exist_ok=True)

        self.max_length = max_length
        self.set = set()
        self.file_count = 0
        self.key = lambda tup: tup[0]

    def __del__(self):
        shutil.rmtree(self.root)

    def add(self, element):
        self.set.add(element)
        if len(self.set) > self.max_length:
            self.save_set()

    def save_set(self):
        if len(self.set) > 0:
            with open(f"{self.root}/{self.file_count}.pkl", "wb") as f:
                pickle.dump(self.set, f)
                self.set.clear()
                self.file_count += 1

    def sort(self, reverse: bool = False):
        self.save_set()

        chunks = [open(f"{self.root}/{c}.pkl", "rb") for c in range(self.file_count)]
        old_root = self.root
        self.root = f"{self.root}_{hash(time.time())}"
        Path(self.root).mkdir(parents=True, exist_ok=True)
        self.file_count = 0

        for elem in merge(*chunks, key=self.key, reverse=reverse):
            self.add(elem)

        shutil.rmtree(old_root)

    def __getitem__(self, indices):
        idx_file = indices // self.max_length
        idx_elem = indices % self.max_length

        return sorted(list(pickle.load(open(f"{self.root}/{idx_file}.pkl", "rb"))), key=self.key)[idx_elem]


if __name__ == '__main__':
    a = BigList(root="tmp", max_length=10000)

    for i in range(int(1e5)):
        a.add((-1*i, -1*i))

    a.sort()

    for i in range(10):
        print(a[i])
