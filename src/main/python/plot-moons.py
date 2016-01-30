import matplotlib.colors as clr
import matplotlib.pyplot as plt
import numpy as np

moons = np.genfromtxt('/tmp/moons.txt', delimiter=' ', names=['id', 'x', 'y'])
plt.figure(1)
plt.scatter(moons['x'], moons['y'], color='r')

parts = np.genfromtxt('/tmp/parts.csv', delimiter=',', names=['id', 'x', 'y', 'c'])
colors = ['black', 'red', 'green', 'blue', 'purple']
plt.figure(2)
plt.scatter(parts['x'], parts['y'], c=parts['c'], cmap=clr.ListedColormap(colors), lw=0)
plt.show(block=True)
