# CSCI 5523 Spring 2026 Homeworks

These are the homework assignments that I did for the CSCI 5523 class in UMN.

One of the main things I can demonstrate with this repository is my knowledge of pyspark and its use for efficiently finding patterns within large datasets.

However, this also means that you have to install all the nessecary dependencies. This is how you do the setup.

## Setup

### Conda environment

First thing to do is to set up the conda environment. To do that, use this command.

```
conda create -n CSCI_5523 python==3.9.12
```

I'm using version 3.9.12 of python as this was the version I used to create these projects.

### Java Version

Installing the right version of java is important. If you want this to work, you need to find a way to install java 8 or java 11.

### Requirements

The `pyspark` library is the big one, however, the `gdown` library is also used for downloading the datasets. The MatrixMultiplication project uses `numpy` to test the implementation's accuracy, and `tqdm` is used in some projects to run unit tests. All of these requirements can be installed using this command:

```
pip install -r requirements.txt
```

## Project Descriptions

This is a rough idea of what each Homework Assignment tests for:

- HW0 was just meant to see if we could install and use pyspark.
- HW1 tested to see if we could use different pyspark functions.
- HW2 tested to see if we could find frequent itemsets
- HW3 tested our ability to find similar sets
- HW4 had us make a small version of a recommender system, where we see how similar different user's tastes are

## Note about the projects

Since we had large datasets, I did not provide them in the repo. However, I did add a script that lets you download them. Before running any of the homework scripts, use this command.

```
python download_data.py
```
