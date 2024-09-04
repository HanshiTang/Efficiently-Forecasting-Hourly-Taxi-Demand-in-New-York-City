[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Yi0Zbe2y)
# MAST30034 Project 1 README.md
- Name: `Hanshi Tang`
- Student ID: `1266337`


## README 

**Research Goal:** My research goal is forecasting Hourly Yellow and Green Taxi Demand in New York City. 

**Timeline:** The timeline for the research area is 2023-07-01 to 2023-12-01. 

To run the pipeline, please visit the `scripts` directory and run the files in order:
1. `downloading_1.py`: This downloads the tlc raw data into the `data/landing` directory.
2. `downloading_2.py`: This downloads the weather raw data into the `data/landing` directory.
3. `downloading_3.py`: This downloads the event raw data into the `data/landing` directory.

To run the notebooks, please visit the `notebook` directory and run the files in order:
4. `1.1 preprocessing_1.ipynb`: This notebook details preprocessing steps for tlc data and outputs it to the `data/raw`.
6. `1.2 preprocessing_2.ipynb`: This notebook details preprocessing steps for weather data and outputs it to the `data/raw`.
7. `1.3 preprocessing_3.ipynb`: This notebook details preprocessing steps for event data and outputs it to the `data/raw`.
9. `1.4 preprocessing_4.ipynb`: This notebook details join of 3 datasets from 1.1, 1.2, 1.3 and outputs it to the `data/curated`.
10. `1.5 preprocessing_5.ipynb`: This notebook details selects features for tlc data from 1.1 and outputs it to the `data/curated`.
11. `EDA_1.ipynb`: This notebook is used to conduct analysis on the curated joined data.
12. `EDA_2.ipynb`: This notebook is used to conduct analysis on the curated tlc data.
13. `model_analysis.ipynb`: This notebook is used for modeling, and its following discussing.
