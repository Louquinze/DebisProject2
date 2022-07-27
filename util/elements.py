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
        self.drive_loc = root + f"_{hash(time.time())}_loc"
        Path(self.root).mkdir(parents=True, exist_ok=True)
        Path(self.drive_loc).mkdir(parents=True, exist_ok=True)

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
                store = pickle.load(open(self.drive_location[key], "rb"))
                store.add(self.file_count)
                with open(self.drive_location[key], "wb") as f:
                    pickle.dump(store, f)
            else:
                self.drive_location[key] = f"{self.drive_loc}/{time.time()}.pkl"
                with open(self.drive_location[key], "wb") as f:
                    pickle.dump({self.file_count}, f)
            self.hash_table[key] = {value}

        if sys.getsizeof(self.hash_table) / 1e6 > self.memory_limit:
            self.save_set()

    def __getitem__(self, item):
        self.save_set()
        if item not in self.drive_location:
            raise KeyError
        store = pickle.load(open(self.drive_location[item], "rb"))
        for i, idx_file in enumerate(store):
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
        shutil.rmtree(self.drive_loc, ignore_errors=True)

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
        self.set = []
        self.file_count = 0
        self.cached_file = None
        self.cache = None
        self.idx = -1
        self.len = None

    def __next__(self):
        self.idx += 1
        if self.idx >= self.len:
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
            self.cache = pickle.load(open(f"{self.root}/{idx_file}.pkl", "rb"))

        self.cached_file = idx_file
        self.cache.insert(idx_elem, value)  # value inserted
        move_object = self.cache[-1]
        del self.cache[-1]
        with open(f"{self.root}/{idx_file}.pkl", "wb") as f:
            pickle.dump(self.cache, f)

        # move values
        indicator = self.max_length == len(self.cache)
        for c in range(idx_file + 1, self.file_count):
            self.cache = pickle.load(open(f"{self.root}/{c}.pkl", "rb"))
            self.cached_file = c
            indicator = self.max_length == len(self.cache)
            self.cache.insert(0, move_object)
            move_object = self.cache[-1]
            del self.cache[-1]
            with open(f"{self.root}/{c}.pkl", "wb") as f:
                pickle.dump(self.cache, f)
        if indicator:
            self.add(move_object)

    def __getitem__(self, indices):
        # Todo remember last indices and load form cash
        idx_file = indices // self.max_length
        idx_elem = indices % self.max_length
        self.save_set()
        if idx_file != self.cached_file:
            self.cache = pickle.load(open(f"{self.root}/{idx_file}.pkl", "rb"))
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
            self.cache = pickle.load(open(f"{self.root}/{self.file_count - 1}.pkl", "rb"))
            self.cached_file = self.file_count - 1

        res = self.cache.pop()
        with open(f"{self.root}/{self.file_count - 1}.pkl", "wb") as f:
            pickle.dump(self.cache, f)

        return res

    def append(self, element):
        self.add(element)

    def set_len(self):
        self.len = len(self)

    def add(self, element):
        self.set.append(element)
        if len(self.set) >= self.max_length:
            self.save_set()

    def save_set(self):
        # Todo balances, that indexing still is true
        if len(self.set) > 0:
            with open(f"{self.root}/{self.file_count}.pkl", "wb") as f:
                pickle.dump(self.set, f)
                del self.set[:]
                self.file_count += 1

    def chunks(self):
        for c in range(self.file_count):
            yield f"{self.root}/{c}.pkl"

    def sort(self, key, reverse: bool = False):
        """
        def sort(lst):
        pointer = [0 for _ in range(len(lst))]
        res = []

        # init
        values = []
        for c in range(len(lst)):
            tmp = lst[c]
            values.append(tmp[0])

        while True:
            idx = values.index(min(values))  # get idx of min value
            res.append(values[idx])  # add smallest value

            tmp = lst[idx]
            if pointer[idx] == len(tmp) - 1:
                del values[idx]
                del pointer[idx]
                del lst[idx]
            else:
                pointer[idx] += 1  # add smallest value
                values[idx] = tmp[pointer[idx]]

            if len(values) == 0:
                break

        return res

        import random
        sort([sorted([random.randint(0, 2) for _ in range(10)]) for _ in range(5)])
        """
        # Todo implement stack sort merge
        #   1. always scan for the "smallest values" firest value. Store it for all arrays to search ...
        self.save_set()

        pointer = [0 for _ in range(self.file_count)]
        res = BigList(self.root, self.max_length)
        files = [f"{self.root}/{c}.pkl" for c in range(self.file_count)]

        for i, file in enumerate(files):  # sort all list
            if i % 500 == 0:
                print(f"sort sub arry {i+1}")
            lst = pickle.load(open(file, "rb"))
            with open(file, "wb") as f:
                pickle.dump(sorted(lst, key=key), f)

        # init
        values = []
        for file in files:
            tmp = pickle.load(open(file, "rb"))
            values.append(tmp[0])

        while True:
            idx = values.index(min(values, key=key))  # get idx of min value
            res.add(values[idx])  # add smallest value
            # yield values[idx]  # add smallest value

            pointer[idx] += 1
            loaded_lst = pickle.load(open(files[idx], "rb"))
            if pointer[idx] == len(loaded_lst):
                print(f"del {files[idx]} from disk")
                del pointer[idx]
                del values[idx]
                del files[idx]
            else:
                values[idx] = loaded_lst[pointer[idx]]
            if len(files) == 0:
                break

        res.set_len()
        res.save_set()
        return res


if __name__ == '__main__':
    import random
    a = BigList(root="tmp", max_length=10)

    for i in range(int(100)):
        a.add((-1 * random.randint(0, 5), -1 * random.randint(0, 5)))
        # a.add((-1 * i, -1 * i))
    a.save_set()
    a = a.sort(key=lambda tup: tup[-1])

    print(len(a))
    for idx, i in enumerate(a):
        print(idx, i)
