# Analyze an eCommerce dataset to practice data ingestion, cleaning, manipulation, 
# and transformation skills using pandas.

import pandas as pd
import datetime as dt

pd.options.mode.chained_assignment = None 
pd.options.display.float_format = '{:,.2f}'.format

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

def transform(manipulated_data):
    # Group the data by product_category and summarize total sales
    total_sales_prod_cat = manipulated_data.groupby("product_category").agg({'total_price': 'sum', 'quantity': 'mean'})

    # Pivot the data to create a summary table showing total sales per month for each product category.
    total_sales_prod_cat_by_month = manipulated_data.groupby(["product_category", "order_month"]).agg({'total_price': 'sum'})

    # Normalize the unit_price column to show prices as a percentage of the average price across all products.
    avg_price_all_products = manipulated_data['unit_price'].mean()
    print(avg_price_all_products)
    manipulated_data['unit_price'] = (manipulated_data['unit_price'] / avg_price_all_products)*100

    return total_sales_prod_cat, total_sales_prod_cat_by_month, manipulated_data

def main():
    raw_data = read_dataset()
    cleaned_data = clean_data(raw_data)
    manipulated_data = manipulate(cleaned_data)
    final_data = transform(manipulated_data)
    print()
    print(final_data[0])
    print()
    print(final_data[1])
    print()
    print(final_data[2])

main()