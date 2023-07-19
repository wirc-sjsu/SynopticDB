# SynopticDB

The goal of this project is to collect data from the Synoptic Mesonet API (Formerly MesoWest) and store the data on a local database where the user can query the exact data they require without limits at a faster speed.

## Getting Started

### Dependencies

Python 3 and package modules
* pandas
* sqlite3
* [synopticPy](https://synopticpy.readthedocs.io/en/latest/user_guide/install.html#option-1-recommended-conda-environment)

### Installation

Install the dependencies using conda:

    conda env create -f environment.yml
    conda activate synoptic

### Example code

Some examples are provided inside the main section of SynopticDB.py and in the Jupyter Notebook TestSynopticDB.ipynb.

## Authors

* jdrucker1
* Fergui

## Acknowledgments

A portion of this work used code generously provided by Brian Blaylock's SynopticPy python package (https://github.com/blaylockbk/SynopticPy)
