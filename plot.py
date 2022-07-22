import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
from matplotlib.pyplot import figure


for idx, file in enumerate(os.listdir("res")):
    if idx == 0:
        df = pd.read_csv(f"res/{file}")
    else:
        df = df.append(pd.read_csv(f"res/{file}"), ignore_index=True)

df.to_csv("res.csv")

# 0: hashjoin, 1: sortmergejoin, 2: hashsortjoin
df["join"].replace(0, "hash_join", inplace=True)  # x
df["join"].replace(1, "parallel_sort_join", inplace=True)
df["join"].replace(2, "hash_sort_join", inplace=True)  # x
df["join"].replace(3, "grace_hash_join", inplace=True)
df["join"].replace(4, "grace_sort_hash_join", inplace=True)

figure(figsize=(10, 6), dpi=80)
ax = sns.boxplot(x="join", y="duration", data=df[df["dataset"] == 0])
plt.title("100k Dataset")
plt.savefig("fig/100k_Dataset.pdf")
plt.cla()

figure(figsize=(10, 6), dpi=80)
ax = sns.boxplot(x="join", y="duration", data=df[(df["dataset"] == 0) & (df["join"] != "parallel_sort_join")])
plt.title("100k Dataset without Parallel Join")
plt.savefig("fig/100k_Dataset_without_p.pdf")
plt.cla()

figure(figsize=(10, 6), dpi=80)
sns.boxplot(x="join", y="duration", data=df[df["dataset"] == 1])
plt.title("10M Dataset")
plt.savefig("fig/10M_Dataset.pdf")
plt.cla()

figure(figsize=(10, 6), dpi=80)
sns.boxplot(x="join", y="duration", data=df[(df["dataset"] == 1) & (df["join"] != "parallel_sort_join")])
plt.title("10M Dataset without Parallel Join")
plt.savefig("fig/10M_Dataset_without_p.pdf")
plt.cla()
