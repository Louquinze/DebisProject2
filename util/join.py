import os
import warnings
import sys


def creat_dict():
    int_dict = dict()
    str_dict = dict()
    with open("data/10M_dictionary.txt") as f:
        for line in f:
            line = line.replace("\n", "")
            line = line.split(":")
            if len(line) != 3:
                continue
            int_dict[f"{line[0]}:{line[1]}"] = int(line[2])
            str_dict[int(line[2])] = f"{line[0]}:{line[1]}"
    return int_dict, str_dict


def get_vertical_partitions(key, int_dict: dict = None, str_dict: dict = None, big_join: bool = False):
    # edges: follows, friendOf, likes, hasReview
    # nodes: User,
    # Todo make dict in other function
    if big_join:  # prepare data
        # create partition
        with open("data/10M_int.txt") as f:
            for line in f:
                try:
                    line = [int(i) for i in line.replace("\n", "").split(" ")]
                    if line[1] == int_dict[key]:
                        yield line[0], line[2]
                except Exception as e:
                    warnings.warn(f"exception: {e}")
    else:
        raise NotImplementedError()


def hashjoin(partition_1, partition_2, memory_limit: int = 2):
    partition_1.set_len()
    partition_2.set_len()

    len_p_1 = len(partition_1)  # Todo this is redundant
    idx_p_1 = 0

    hash_table = dict()

    while True:
        if idx_p_1 >= len_p_1 - 1:
            break
        # Add tuple to hash_table
        if partition_1[idx_p_1][-1] in hash_table:
            hash_table[partition_1[idx_p_1][-1]].add(partition_1[idx_p_1][:-1])
        else:
            hash_table[partition_1[idx_p_1][-1]] = {partition_1[idx_p_1][:-1]}

        # if in memory table exceeds limit
        if sys.getsizeof(hash_table) / 1e6 > memory_limit:
            # scan p_2 and yield the points
            for elem in partition_2:
                if elem[0] in hash_table:
                    for i in hash_table[elem[0]]:
                        yield *i, elem[0], *elem[1:]
            del hash_table
        idx_p_1 += 1

    for elem in partition_2:
        if elem[0] in hash_table:
            for i in hash_table[elem[0]]:
                yield *i, elem[0], *elem[1:]
    del hash_table


def sortmergejoin(partition_1, partition_2):
    # Todo apdate this that i can use any object with inplace sorting
    partition_1.set_key(key=lambda tup: tup[-1])  # sort first part with respect to the object
    partition_2.set_key(key=lambda tup: tup[0])  # sort second part with respect to the subject
    partition_1.sort()
    partition_2.sort()

    len_p_1 = len(partition_1) - 1
    len_p_2 = len(partition_2) - 1

    idx_p_1 = 0
    idx_p_2 = 0
    mark_p_2 = None

    while True:
        if idx_p_1 >= len_p_1 or idx_p_2 >= len_p_2:  # breaking condition, Todo len - 1 ?
            break
        if partition_1[idx_p_1][-1] < partition_2[idx_p_2][0]:
            idx_p_1 += 1  # advance p_1
            if mark_p_2 is not None:
                idx_p_2 = mark_p_2  # rest p_2
            mark_p_2 = None
        elif partition_1[idx_p_1][-1] > partition_2[idx_p_2][0]:
            idx_p_2 += 1  # advance p_2
        elif partition_1[idx_p_1][-1] == partition_2[idx_p_2][0]:
            yield *partition_1[idx_p_1], *partition_2[idx_p_2][1:]
            if mark_p_2 is None:
                mark_p_2 = idx_p_2
            idx_p_2 += 1
