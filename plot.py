import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

for idx, file in enumerate(os.listdir("res")):
    if idx == 0:
        df = pd.read_csv(f"res/{file}")
    else:
        df = df.append(pd.read_csv(f"res/{file}"), ignore_index=True)

sns.boxplot(x="join", y="duration", data=df[df["dataset"] == 0])
plt.show()

sns.boxplot(x="join", y="duration", data=df[df["dataset"] == 1])
plt.show()
