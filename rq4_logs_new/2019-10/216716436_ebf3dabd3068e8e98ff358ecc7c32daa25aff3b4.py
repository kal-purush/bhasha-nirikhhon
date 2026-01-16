import matplotlib.pyplot as plt
import seaborn as sns
sns.set()


def cluster_analysis(train_data, target_data):
    for columns in train_data:
        plt.scatter(train_data[columns], target_data)
        # plt.xlabel(train_data[columns])
        # plt.ylabel(target_data.columns.value)
        plt.show()