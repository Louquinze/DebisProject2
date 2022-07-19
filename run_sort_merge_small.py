from util.elements import BigList
from util.join import creat_dict, get_vertical_partitions, sortmergejoin


if __name__ == '__main__':
    int_dict, str_dict = creat_dict()
    follows = BigList(root="follows", max_length=int(1e07))
    for elem in get_vertical_partitions(key="wsdbm:follows", int_dict=int_dict,
                                        str_dict=str_dict, big_join=False):
        follows.add(elem)

    friendOf = BigList(root="friendOf", max_length=int(1e07))
    for elem in get_vertical_partitions(key="wsdbm:friendOf", big_join=False,
                                        int_dict=int_dict, str_dict=str_dict):
        friendOf.add(elem)

    Join_1 = BigList(root="Join_1", max_length=int(1e07))
    for idx, elem in enumerate(sortmergejoin(follows, friendOf)):
        Join_1.add(elem)
        if idx % int(1e06) == 0:
            print(idx, elem)
    del follows
    del friendOf
    print("finished Join_1")

    likes = BigList(root="friendOf", max_length=int(1e07))
    for elem in get_vertical_partitions(key="wsdbm:likes", big_join=False,
                                        int_dict=int_dict, str_dict=str_dict):
        likes.add(elem)

    hasReview = BigList(root="hasReview", max_length=int(1e07))
    for elem in get_vertical_partitions(key="rev:hasReview", big_join=False,
                                     int_dict=int_dict, str_dict=str_dict):
        hasReview.add(elem)

    Join_2 = BigList(root="Join_2", max_length=int(1e07))
    for elem in sortmergejoin(likes, hasReview):
        Join_2.add(elem)
    del likes
    del hasReview
    print("finished Join_2")

    for idx, elem in enumerate(sortmergejoin(Join_1, Join_2)):
        idx_save = idx
        if idx % int(1e06) == 0:
            print(idx, elem)