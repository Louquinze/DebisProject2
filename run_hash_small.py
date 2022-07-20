from util.elements import BigList
from util.join import creat_dict, get_vertical_partitions, hashjoin
import time

if __name__ == '__main__':
    start = time.time()
    int_dict, str_dict = creat_dict()
    follows = BigList(root="hash_small_follows", max_length=int(1e07))
    for elem in get_vertical_partitions(key="wsdbm:follows", int_dict=int_dict,
                                        str_dict=str_dict, big_join=False):
        follows.add(elem)

    friendOf = BigList(root="hash_small_friendOf", max_length=int(1e07))
    for elem in get_vertical_partitions(key="wsdbm:friendOf", big_join=False,
                                        int_dict=int_dict, str_dict=str_dict):
        friendOf.add(elem)

    Join_1 = BigList(root="hash_small_Join_1", max_length=int(1e07))
    for idx, elem in enumerate(hashjoin(follows, friendOf, memory_limit=4)):
        Join_1.add(elem)
        if idx % int(1e06) == 0:
            print(idx, elem)
    del follows
    del friendOf
    print("finished Join_1")

    likes = BigList(root="hash_small_friendOf", max_length=int(1e07))
    for elem in get_vertical_partitions(key="wsdbm:likes", big_join=False,
                                        int_dict=int_dict, str_dict=str_dict):
        likes.add(elem)

    hasReview = BigList(root="hash_small_hasReview", max_length=int(1e07))
    for elem in get_vertical_partitions(key="rev:hasReview", big_join=False,
                                        int_dict=int_dict, str_dict=str_dict):
        hasReview.add(elem)

    Join_2 = BigList(root="hash_small_Join_2", max_length=int(1e07))
    for elem in hashjoin(likes, hasReview, memory_limit=4):
        Join_2.add(elem)
        if idx % int(1e06) == 0:
            print(idx, elem)
    del likes
    del hasReview
    print("finished Join_2")

    for idx, elem in enumerate(hashjoin(Join_1, Join_2, memory_limit=4)):
        idx_save = idx
        if idx % int(1e06) == 0:
            print(idx, elem)

    end = time.time()
    duration = end - start
    print(f"Time hash join small: {duration // 3600}h  {(duration // 60) % 60}min  {duration % 60}sec")

