# %%
from matplotlib.pyplot import *
import numpy as np
style.use('seaborn-dark')

# %%
x = np.linspace(1, 99, 100)
y = np.random.rand(100, 1)
plot(x, y)

# %% Ex1


def draw_plot(x, y):
    plot(x, y, color="red", linestyle="-")


x = np.linspace(1, 99, 100)
y = np.random.rand(100, 1)

draw_plot(x, y)

# %% Ex2


def plot_sin_cos():
    x = np.linspace(-2*np.pi, 2*np.pi)
    y_sin = np.sin(x)
    y_cos = np.cos(x)

    draw_plot(x, y_sin)
    draw_plot(x, y_cos)


plot_sin_cos()

# %% Ex3


def plot_scatter(x, y):
    scatter(x, y, marker="x", color="green")


x = np.linspace(1, 99, 100)
y = np.random.rand(100, 1)
plot_scatter(x, y)

# %% Ex4


def plot_bar(x, height):
    bar(x, height, facecolor="pink", edgecolor="red")


x = np.linspace(1, 99, 100)
height = np.random.randint(100, size=100)
plot_bar(x, height)


# %%Ex5
x_str = np.array(["a", "b", "c"])
height = np.random.randint(100, size=3)
plot_bar(x_str, height)


# %% Ex6
def draw_mean_std(annots, values):
    values_mean = np.mean(values, axis=0)
    values_std = np.std(values, axis=0)

    bar(annots, values_mean, yerr=values_std)


m = 10
n = 2
annots = np.linspace(1, 11, m)
values = np.random.randint(100, size=(n, m))
draw_mean_std(annots, values)


# %% Ex7
def draw_hist(x, nb_groups):
    hist(x, bins=nb_groups, histtype='step')


x = np.random.randint(100, size=30)
draw_hist(x, 4)

# %% Ex 8


def show_boxplot(x):
    boxplot(x)


x = np.random.randint(10, size=(10, 5))
show_boxplot(x)

# %%


def show_violin_plot(x):
    violinplot(x)


show_violin_plot(x)
# %%Ex10


def draw_bubblechart(x, y, z):
    scatter(x, y, s=z, c=z)


n = 100
x = np.random.randint(100, size=n)
y = np.random.randint(100, size=n)
z = np.random.randint(100, size=n)

draw_bubblechart(x, y, z)


# %%
def draw_scatter_matrix(x):
    n, m = x.shape
    fig, axes = subplots(nrows=n, ncols=n)

    for i in range(n):
        for j in range(n):
            current_axes = axes[i, j]
            # plot histogram on diagonal
            if i == j:
                current_axes.hist(x[i])
            else:
                current_axes.scatter(x[i], x[j])
            current_axes.axes.get_xaxis().set_visible(False)
            current_axes.axes.get_yaxis().set_visible(False)


x = np.random.randint(100, size=(4, 6))
draw_scatter_matrix(x)

# %% Ex13

