from util.elements import BigList
from util.join import creat_dict, get_vertical_partitions, hashjoin


if __name__ == '__main__':
    int_dict, str_dict = creat_dict()
    follows = BigList(root="follows", max_length=int(1e07))
    for elem in get_vertical_partitions(key="<http://db.uwaterloo.ca/~galuc/wsdbm/follows>", int_dict=int_dict,
                                        str_dict=str_dict, big_join=True):
        follows.add(elem)

    friendOf = BigList(root="friendOf", max_length=int(1e07))
    for elem in get_vertical_partitions(key="<http://db.uwaterloo.ca/~galuc/wsdbm/friendOf>", big_join=True,
                                        int_dict=int_dict, str_dict=str_dict):
        friendOf.add(elem)

    Join_1 = BigList(root="Join_1", max_length=int(1e07))
    for idx, elem in enumerate(hashjoin(follows, friendOf)):
        Join_1.add(elem)
        if idx % int(1e06) == 0:
            print(idx, elem)
    del follows
    del friendOf
    print("finished Join_1")

    likes = BigList(root="friendOf", max_length=int(1e07))
    for elem in get_vertical_partitions(key="<http://db.uwaterloo.ca/~galuc/wsdbm/likes>", big_join=True,
                                        int_dict=int_dict, str_dict=str_dict):
        likes.add(elem)

    hasReview = BigList(root="hasReview", max_length=int(1e07))
    for elem in get_vertical_partitions(key="<http://purl.org/stuff/rev#hasReview>", big_join=True,
                                     int_dict=int_dict, str_dict=str_dict):
        hasReview.add(elem)

    Join_2 = BigList(root="Join_2", max_length=int(1e07))
    for elem in hashjoin(likes, hasReview):
        Join_2.add(elem)
    del likes
    del hasReview
    print("finished Join_2")

    for idx, elem in enumerate(hashjoin(Join_1, Join_2)):
        idx_save = idx
        if idx % int(1e06) == 0:
            print(idx, elem)
    print(idx_save)