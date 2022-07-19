import warnings
import sys
import pickle
from elements import BigList


def feedfile(i, fname):
    fname += f"{i}.pkl"
    with open(fname, "rb") as f:
        count = pickle.load(f)
        for _ in range(count):
            yield pickle.load(f)


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


def sortmergejoin(partition_1, partition_2):
    partition_1.set_key(key=lambda tup: tup[-1])  # sort first part with respect to the object
    partition_2.set_key(key=lambda tup: tup[0])  # sort second part with respect to the subject
    partition_1.sort()
    partition_2.sort()

    len_p_1 = len(partition_1)
    len_p_2 = len(partition_2)

    idx_p_1 = 0
    idx_p_2 = 0
    mark_p_2 = None

    while True:
        if idx_p_1 == len_p_1 or idx_p_2 == len_p_2:  # breaking condition
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


if __name__ == '__main__':
    int_dict, str_dict = creat_dict()
    follows = BigList(root="follows", max_length=int(1e06))
    for elem in get_vertical_partitions(key="<http://db.uwaterloo.ca/~galuc/wsdbm/follows>", int_dict=int_dict,
                                        str_dict=str_dict, big_join=True):
        follows.add(elem)

    friendOf = BigList(root="friendOf", max_length=int(1e06))
    for elem in get_vertical_partitions(key="<http://db.uwaterloo.ca/~galuc/wsdbm/friendOf>", big_join=True,
                                        int_dict=int_dict, str_dict=str_dict):
        friendOf.add(elem)

    Join_1 = BigList(root="Join_1", max_length=int(1e06))
    for elem in sortmergejoin(follows, friendOf):
        # Join_1.add(elem)
        print(elem)
    del follows
    del friendOf
    print("finished Join_1")
    """
    likes = BigList(root="friendOf", max_length=int(1e06))
    for elem in get_vertical_partitions(key="<http://db.uwaterloo.ca/~galuc/wsdbm/likes>", big_join=True,
                                        int_dict=int_dict, str_dict=str_dict):
        likes.add(elem)

    hasReview = BigList(root="hasReview", max_length=int(1e06))
    for elem in get_vertical_partitions(key="<http://purl.org/stuff/rev#hasReview>", big_join=True,
                                     int_dict=int_dict, str_dict=str_dict):
        hasReview.add(elem)

    Join_2 = BigList(root="Join_2", max_length=int(1e06))
    for elem in sortmergejoin(likes, hasReview):
        Join_2.add(elem)
    del likes
    del hasReview
    print("finished Join_1")"""

    # for elem in sortmergejoin(Join_1, Join_2):
    #     print(elem)


