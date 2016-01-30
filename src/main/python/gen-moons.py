import numpy as np
from sklearn import datasets

np.random.seed(0)
n_samples = 100000
noisy_moons = datasets.make_moons(n_samples=n_samples, noise=.05)
X, _ = noisy_moons
with open("/tmp/moons.txt", "wb") as fw:
    for i in range(n_samples):
        fw.write("{} {} {}\n".format(i, X[i][0] * 100, X[i][1] * 100))
