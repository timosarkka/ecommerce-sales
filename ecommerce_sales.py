# Objective: Analyze an eCommerce dataset to practice data ingestion, cleaning, manipulation, 
# and transformation skills using pandas.

import pandas as pd
import datetime as dt

pd.options.mode.chained_assignment = None

# Display all columns
pd.set_option('display.max_columns', None)

# Read in the data set
def read_dataset():
    df = pd.read_csv("ecommerce_sales.csv")
    return df

# Clean the data by dropping the NaN rows
def clean_data(raw_data):
    cleaned_data = raw_data.dropna()
    return cleaned_data

# Create a new column total_price, add order_weekday and order_month to separate columns
def manipulate(cleaned_data):

    # Create total price column
    cleaned_data["total_price"] = cleaned_data["quantity"] * cleaned_data["unit_price"]

    # Create new column with day of the week
    cleaned_data["order_date"] = pd.to_datetime(cleaned_data["order_date"])
    cleaned_data["order_weekday"] = cleaned_data["order_date"].dt.day_name()

    # Create new column with month of the year
    cleaned_data["order_month"] = cleaned_data["order_date"].dt.month_name()
    return cleaned_data

# Group the data by product_category and summarize total sales
def transform(manipulated_data):
    final_data = manipulated_data.groupby("product_category").agg({'total_price': 'sum', 'quantity': 'mean'})
    return final_data

def main():
    raw_data = read_dataset()
    cleaned_data = clean_data(raw_data)
    manipulated_data = manipulate(cleaned_data)
    final_data = transform(manipulated_data)
    print(final_data)

main()