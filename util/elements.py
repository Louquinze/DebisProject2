import time
import os
import shutil
import sys
import pickle
from pathlib import Path


# from util.heapq_adapt import merge


class BigDict:
    # if sys.getsizeof(hash_table) / 1e6 > memory_limit:
    def __init__(self, root, memory_limit: int = 2):
        self.root = root + f"_{hash(time.time())}"
        Path(self.root).mkdir(parents=True, exist_ok=True)

        self.memory_limit = memory_limit
        self.hash_table = dict()
        self.drive_location = dict()
        self.file_count = 0
        self.loaded_file = None
        self.cache = None

    def __setitem__(self, key, value):
        if key in self.hash_table:
            self.hash_table[key].add(value)
        else:
            if key in self.drive_location:
                self.drive_location[key].add(self.file_count)
            else:
                self.drive_location[key] = {self.file_count}
            self.hash_table[key] = {value}

        if sys.getsizeof(self.hash_table) / 1e6 > self.memory_limit:
            self.save_set()

    def __getitem__(self, item):
        self.save_set()
        if item not in self.drive_location:
            raise KeyError
        for i, idx_file in enumerate(self.drive_location[item]):
            if self.loaded_file != idx_file:
                self.loaded_file = idx_file
                self.cache = pickle.load(open(f"{self.root}/{idx_file}.pkl", "rb"))
            if i == 0:
                res = self.cache[item]
            else:
                res = res | self.cache[item]
        return res

    def __del__(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def save_set(self):
        # Todo balances, that indexing still is true
        if len(self.hash_table) > 0:
            with open(f"{self.root}/{self.file_count}.pkl", "wb") as f:
                pickle.dump(self.hash_table, f)
                self.hash_table.clear()
                self.file_count += 1

    def keys(self):
        for key in self.drive_location.keys():
            yield key


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
        if self.idx >= self.len - 1:
            self.idx = -1
            raise StopIteration
        else:
            return self[self.idx]

    def __iter__(self):
        return self

    def __del__(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def __setitem__(self, indices, value):
        idx_file = indices // self.max_length
        idx_elem = indices % self.max_length
        self.save_set()

        if idx_file != self.cached_file:
            self.cache = sorted(list(pickle.load(open(f"{self.root}/{idx_file}.pkl", "rb"))), key=self.key)

        self.cached_file = idx_file
        self.cache.insert(idx_elem, value)  # value inserted
        move_object = self.cache[-1]
        del self.cache[-1]
        with open(f"{self.root}/{idx_file}.pkl", "wb") as f:
            pickle.dump(set(self.cache), f)

        # move values
        indicator = self.max_length == len(self.cache)
        for c in range(idx_file + 1, self.file_count):
            self.cache = sorted(list(pickle.load(open(f"{self.root}/{c}.pkl", "rb"))), key=self.key)
            self.cached_file = c
            indicator = self.max_length == len(self.cache)
            self.cache.insert(0, move_object)
            move_object = self.cache[-1]
            del self.cache[-1]
            with open(f"{self.root}/{c}.pkl", "wb") as f:
                pickle.dump(set(self.cache), f)
        if indicator:
            self.add(move_object)

    def __getitem__(self, indices):
        # Todo remember last indices and load form cash
        idx_file = indices // self.max_length
        idx_elem = indices % self.max_length
        self.save_set()
        if idx_file != self.cached_file:
            self.cache = sorted(list(pickle.load(open(f"{self.root}/{idx_file}.pkl", "rb"))), key=self.key)
            self.cached_file = idx_file
        return self.cache[idx_elem]  # -1 ?

    def __len__(self):
        # Todo optimize this
        self.save_set()
        if self.file_count > 1:
            lenght = self.max_length * (self.file_count - 2)
            lenght += len(pickle.load(open(f"{self.root}/{self.file_count - 1}.pkl", "rb")))
        else:
            lenght = len(pickle.load(open(f"{self.root}/{self.file_count - 1}.pkl", "rb")))

        return lenght

    def pop(self):
        self.save_set()

        if self.file_count - 1 != self.cached_file:
            self.cache = sorted(list(pickle.load(open(f"{self.root}/{self.file_count - 1}.pkl", "rb"))), key=self.key)
            self.cached_file = self.file_count - 1

        res = self.cache.pop()
        with open(f"{self.root}/{self.file_count - 1}.pkl", "wb") as f:
            pickle.dump(set(self.cache), f)

        return res

    def append(self, element):
        self.add(element)

    def set_len(self):
        self.len = len(self)

    def add(self, element):
        self.set.add(element)
        if len(self.set) >= self.max_length:
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

    def chunks(self):
        for c in range(self.file_count):
            yield f"{self.root}/{c}.pkl"

    def sort(self, reverse: bool = False):
        """
        self.cached_file = None
        self.save_set()

        chunks = [f"{self.root}/{c}.pkl" for c in range(self.file_count)]
        old_root = self.root
        self.root = f"{self.root}_{hash(time.time())}"
        Path(self.root).mkdir(parents=True, exist_ok=True)
        self.file_count = 0

        for elem in merge(chunks, key=self.key, reverse=reverse):
            self.add(elem)

        shutil.rmtree(old_root, ignore_errors=True)
        """
        # Todo implement stack sort merge
        #   1. always scan for the "smallest values" firest value. Store it for all arrays to search ...
        self.save_set()

        pointer = [0 for _ in range(self.file_count)]
        skips = [False for _ in range(self.file_count)]
        res = BigList(self.root, self.max_length)

        value = None
        idx = None

        while True:
            if idx is None:
                values = []
                for c in range(self.file_count):
                    tmp = pickle.load(open(f"{self.root}/{c}.pkl", "rb"))
                    tmp = sorted(list(tmp), key=self.key)
                    if len(tmp) == pointer[c]:  # ignore file
                        values.append(None)
                        skips[c] = True
                    else:
                        values.append(tmp[pointer[c]])
            else:
                tmp = pickle.load(open(f"{self.root}/{idx}.pkl", "rb"))
                tmp = sorted(list(tmp), key=self.key)
                if len(tmp) == pointer[idx]:  # ignore file
                    values[idx] = None
                    skips[idx] = True
                    # pointer[idx] -= 1
                else:
                    values[idx] = tmp[pointer[idx]]

            start_idx = 0
            for idx_i, value_i in enumerate(values):
                if skips[idx_i]:
                    start_idx = idx_i
                    continue
                if idx_i == start_idx + 1:
                    value = value_i
                    idx = idx_i
                elif value is not None:
                    if self.key(value) > self.key(value_i):
                        value = value_i
                        idx = idx_i
            if sum(skips) == len(skips):
                break
            else:
                res.add(value)
            pointer[idx] += 1

        res.save_set()
        return res


if __name__ == '__main__':
    import random
    a = BigList(root="tmp", max_length=10)

    for i in range(int(100)):
        a.add((-1 * random.randint(0, 10), -1 * random.randint(0, 10)))

    a.save_set()
    a.set_key(lambda tup: tup[-1])
    a = a.sort()

    a.set_len()
    for i in a:
        print(i)
