[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Yi0Zbe2y)
# MAST30034 Project 1 README.md
- Name: `Hanshi Tang`
- Student ID: `1266337`

## Student Instructions
You **must** write up `README.md` for this repository to be eligable for readability marks.

4. Students must store all curated / transformed data in the `data/curated` folder. This will be in the `.gitignore` so **do not upload any raw data files whatsoever**. We will be running your code from the `scripts` directory to regenerate these.
6. Finally, your report `.tex` files must be inside the `report` directory. If you are using overleaf, you can download the `.zip` and extract it into this folder.
8. Add your relevant `requirements.txt` to the root directory. If you are unsure, run `pip3 list --format=freeze > requirements.txt` (or alternative) and copy the output to the repository.
10. When you have read this, delete the `Student Instructions` section to clean the readme up.

Remember, we will be reading through and running your code, so it is in _your best interest_ to ensure it is readable and efficient.

## README example

**Research Goal:** My research goal is ana 

**Timeline:** The timeline for the research area is 2023-12-01 to 2024-05-31

To run the pipeline, please visit the `scripts` directory and run the files in order:
1. `downloading_1.py`: This downloads the tlc raw data into the `data/landing` directory.
2. `downloading_2.py`: This downloads the weather raw data into the `data/landing` directory.
3. `downloading_3.py`: This downloads the event raw data into the `data/landing` directory.
4. 
5. `1.1 preprocessing_1.ipynb`: This notebook details preprocessing steps for tlc data and outputs it to the `data/raw`.
6. `1.2 preprocessing_2.ipynb`: This notebook details preprocessing steps for weather data and outputs it to the `data/raw`.
7. `1.3 preprocessing_3.ipynb`: This notebook details preprocessing steps for event data and outputs it to the `data/raw`.
8. `1.4 feature engineering.ipynb`: This notebook performs feature engineering for tlc data and outputs it to the `data/raw`
9. `1.5 preprocessing_4.ipynb`: This notebook details preprocessing steps for event data and outputs it to the `data/curated`.
10. `analysis.ipynb`: This notebook is used to conduct analysis on the curated data.
11. `model.py` and `model_analysis.ipynb`: The script is used to run the model from CLI and the notebook is used for analysing and discussing the model.
