# import matplotlib as plt
import matplotlib.pyplot as plt
from sklearn.datasets.samples_generator import make_blobs

# creating datasets X containing n_samples
# Y containing two classes
# print(make_blobs)
input()
X, Y = make_blobs(n_samples=500, centers=2,
                  random_state=0, cluster_std=0.40)


# plotting scatters
plt.scatter(X[:, 0], X[:, 1], c=Y, s=50, cmap='spring');
plt.show()