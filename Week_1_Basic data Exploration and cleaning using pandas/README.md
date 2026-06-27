# Basic data Exploration and cleaning using pandas

## Objective

The objective of this assignment is to perform basic data exploration and cleaning operations on a dataset using Python and Pandas.

## Dataset

Dataset Name: Combined_dataset.csv

## Tasks Performed

### 1. Load Dataset

-> Loaded the CSV dataset into a Pandas DataFrame using `pd.read_csv()`.

### 2. Explore Data

-> Displayed the first 5 rows using `head()`.
-> Displayed the last 5 rows using `tail()`.
-> Checked the dataset shape using `shape`.
-> Viewed column names using `columns`.
-> Examined data types using `dtypes`.

### 3. Handle Missing Values

-> Identified missing values using `isnull().sum()`.
-> Filled missing values using forward fill (`ffill()`).

### 4. Basic Data Operations

-> Filtered records where rating is greater than 0.
-> Selected relevant columns:

  . product_id
  . title
  . initial_price
  . final_price
  . rating
  . category

### 5. Remove Duplicates

* Removed duplicate records using `drop_duplicates()`.

### 6. Create Derived Column

* Added a new column `quantity` with value 1.
* Created a derived column:

  total_amount = final_price × quantity

### 7. Save Cleaned Dataset

-> Exported the cleaned dataset as `cleaned_dataset.csv`.

## Libraries Used

-> Pandas

## Output Files

-> Assignment Notebook (`.ipynb`)
-> Cleaned Dataset (`cleaned_dataset.csv`)
-> README.md

## Conclusion

The dataset was successfully explored, cleaned, transformed, and saved. Basic data preprocessing techniques such as handling missing values, filtering data, removing duplicates, and creating derived columns were applied using Pandas.
