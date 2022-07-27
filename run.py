from util.elements import BigList
from util.join import creat_dict, get_vertical_partitions, hashjoin, sortmergejoin, gracehashjoin, hashsortjoin
import time

import argparse
from pathlib import Path

parser = argparse.ArgumentParser(description='Experiment Parser')
parser.add_argument('--id', type=int, default=0)
parser.add_argument('--join', type=int, default=0)  # 0: hashjoin, 1: sortmergejoin, 2: hashsortjoin
parser.add_argument('--dataset', type=int, default=0)  # 0: small, 1: huge
parser.add_argument('--max_length', type=int, default=int(1e06))
parser.add_argument('--memory_limit', type=int, default=2)

args = parser.parse_args()

if __name__ == '__main__':
    start = time.time()
    int_dict, str_dict = creat_dict()
    if args.dataset == 1:
        follows = BigList(root=f"fo_{args.id}_{args.join}_{args.dataset}_{args.max_length}_{args.memory_limit}",
                          max_length=args.max_length)
        key = "<http://db.uwaterloo.ca/~galuc/wsdbm/follows>"
        big_join = True
    elif args.dataset == 0:
        follows = BigList(root=f"fo_{args.id}_{args.join}_{args.dataset}_{args.max_length}_{args.memory_limit}",
                          max_length=args.max_length)
        key = "wsdbm:follows"
        big_join = False
    for elem in get_vertical_partitions(key=key, int_dict=int_dict,
                                        str_dict=str_dict, big_join=big_join):
        follows.add(elem)

    if args.dataset == 1:
        friendOf = BigList(root=f"fr_{args.id}_{args.join}_{args.dataset}_{args.max_length}_{args.memory_limit}",
                           max_length=args.max_length)
        key = "<http://db.uwaterloo.ca/~galuc/wsdbm/friendOf>"
        big_join = True
    elif args.dataset == 0:
        friendOf = BigList(root=f"fr_{args.id}_{args.join}_{args.dataset}_{args.max_length}_{args.memory_limit}",
                           max_length=args.max_length)
        key = "wsdbm:friendOf"
        big_join = False
    for elem in get_vertical_partitions(key=key, int_dict=int_dict,
                                        str_dict=str_dict, big_join=big_join):
        friendOf.add(elem)

    Join_1 = BigList(root=f"j_1_{args.id}_{args.join}_{args.dataset}_{args.max_length}_{args.memory_limit}",
                     max_length=args.max_length)

    # 0: hashjoin, 1: sortmergejoin, 2: hashsortjoin
    if args.join == 0:
        join = hashjoin(follows, friendOf, memory_limit=args.memory_limit)
    elif args.join == 1:
        join = sortmergejoin(follows, friendOf)
    elif args.join == 2:
        join = hashsortjoin(follows, friendOf, memory_limit=args.memory_limit)
    elif args.join == 3:
        join = gracehashjoin(follows, friendOf, memory_limit=args.memory_limit)
    elif args.join == 4:
        join = gracehashjoin(follows, friendOf, memory_limit=args.memory_limit, sorted=True)

    for idx, elem in enumerate(join):
        Join_1.add(elem)
        if idx % int(1e06) == 0:
            print(idx, elem)
    del follows
    del friendOf

    Join_1.save_set()
    print("finished Join_1")
    # __________________________________________________________________________________________________________________
    if args.dataset == 1:
        likes = BigList(root=f"l_{args.id}_{args.join}_{args.dataset}_{args.max_length}_{args.memory_limit}",
                        max_length=args.max_length)
        key = "<http://db.uwaterloo.ca/~galuc/wsdbm/likes>"
        big_join = True
    elif args.dataset == 0:
        likes = BigList(root=f"l_{args.id}_{args.join}_{args.dataset}_{args.max_length}_{args.memory_limit}",
                        max_length=args.max_length)
        key = "wsdbm:likes"
        big_join = False

    for elem in get_vertical_partitions(key=key, int_dict=int_dict, str_dict=str_dict, big_join=big_join):
        likes.add(elem)

    if args.dataset == 1:
        hasReview = BigList(root=f"r_{args.id}_{args.join}_{args.dataset}_{args.max_length}_{args.memory_limit}",
                            max_length=args.max_length)
        key = "<http://purl.org/stuff/rev#hasReview>"
        big_join = True
    elif args.dataset == 0:
        hasReview = BigList(root=f"r_{args.id}_{args.join}_{args.dataset}_{args.max_length}_{args.memory_limit}",
                            max_length=args.max_length)
        key = "rev:hasReview"
        big_join = False

    for elem in get_vertical_partitions(key=key, int_dict=int_dict, str_dict=str_dict, big_join=big_join):
        hasReview.add(elem)

    # 0: hashjoin, 1: sortmergejoin, 3: hashsortjoin
    if args.join == 0:
        join = hashjoin(likes, hasReview, memory_limit=args.memory_limit)
    elif args.join == 1:
        join = sortmergejoin(likes, hasReview)
    elif args.join == 2:
        join = hashsortjoin(likes, hasReview, memory_limit=args.memory_limit)
    elif args.join == 3:
        join = gracehashjoin(likes, hasReview, memory_limit=args.memory_limit)
    elif args.join == 4:
        join = gracehashjoin(likes, hasReview, memory_limit=args.memory_limit, sorted=True)

    Join_2 = BigList(root=f"j_2_{args.id}_{args.join}_{args.dataset}_{args.max_length}_{args.memory_limit}",
                     max_length=args.max_length)

    for idx, elem in enumerate(join):
        Join_2.add(elem)
        if idx % int(1e06) == 0:
            print(idx, elem)
    del likes
    del hasReview

    Join_2.save_set()
    print("finished Join_2")

    # 0: hashjoin, 1: sortmergejoin, 3: hashsortjoin
    if args.join == 0:
        join = hashjoin(Join_1, Join_2, memory_limit=args.memory_limit)
    elif args.join == 1:
        join = sortmergejoin(Join_1, Join_2)
    elif args.join == 2:
        join = hashsortjoin(Join_1, Join_2, memory_limit=args.memory_limit)
    elif args.join == 3:
        join = gracehashjoin(Join_1, Join_2, memory_limit=args.memory_limit)
    elif args.join == 4:
        join = gracehashjoin(Join_1, Join_2, memory_limit=args.memory_limit, sorted=True)

    for idx, elem in enumerate(join):
        idx_save = idx
        if idx % int(1e06) == 0:
            print(idx, elem)
    print(idx_save)

    end = time.time()
    duration = end - start
    Path("res").mkdir(parents=True, exist_ok=True)
    with open(f"res/{args.id}_{args.join}_{args.dataset}_{args.max_length}_{args.memory_limit}.csv", "w") as f:
        f.write(f"h,min,sec,duration,id,join,dataset\n"
                f"{duration // 3600},{(duration // 60) % 60},{duration % 60},{duration},{args.id},{args.join},{args.dataset}")