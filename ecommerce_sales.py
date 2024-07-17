# A short python program for ingestion, cleaning, manipulation and transformation of an e-commerce sales data set.

# Import the necessary libraries
import pandas as pd
import datetime as dt
from tabulate import tabulate

# Set the display options
pd.options.mode.chained_assignment = None 
pd.options.display.float_format = '{:,.2f}'.format
pd.set_option('display.max_columns', None)

# Read in the data set
def read_dataset():
    df = pd.read_csv("ecommerce_sales.csv")
    return df

# Clean the data by dropping null rows
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

    # Group the data by product_category and summarize total sales
    # Order by total sales
    total_sales_prod_cat = manipulated_data.groupby("product_category").agg({'quantity': 'sum', 'total_price': 'sum'}).reset_index()
    total_sales_prod_cat = total_sales_prod_cat.round(2)
    total_sales_prod_cat = total_sales_prod_cat.sort_values(by="total_price", ascending=False)

    # Pivot the data to create a summary table showing total sales per month for each product category.
    total_sales_prod_cat_by_month = manipulated_data.groupby(["product_category", "order_month"]).agg({'total_price': 'sum'}).reset_index()
    total_sales_prod_cat_by_month = total_sales_prod_cat_by_month.sort_values(by="total_price", ascending=False)

    # Normalize the unit_price column to show prices as a percentage of the average price across all products.
    avg_price_all_products = manipulated_data['unit_price'].mean()
    manipulated_data['unit_price_perc'] = (manipulated_data['unit_price'] / avg_price_all_products)*100

    # Drop some columns that are not needed in the last dataframe
    columns_to_drop = ["quantity", "order_date", "country"]  # Example columns to drop
    manipulated_data = manipulated_data.drop(columns=columns_to_drop)

    return total_sales_prod_cat, total_sales_prod_cat_by_month, manipulated_data

# Main program that executes the functions and prints the final dataset
# Using tabulate for prettier printing
def main():
    raw_data = read_dataset()
    cleaned_data = clean_data(raw_data)
    manipulated_data = manipulate(cleaned_data)
    total_sales_prod_cat, total_sales_prod_cat_by_month, normalized_data = transform(manipulated_data)

    print()
    print("This is a short python program for ingestion, cleaning, manipulation and transformation of an e-commerce sales dataset.")
    
    print()
    print("Total Sales by Product Category:")
    print()
    table1 = tabulate(total_sales_prod_cat, headers="keys", tablefmt="github")
    print(table1)

    print("\nTotal Sales by Product Category by Month:")
    print()
    table2 = tabulate(total_sales_prod_cat_by_month, headers="keys", tablefmt="github")
    print(table2)

    print("\nNormalized Sales Data:")
    print()
    table3 = tabulate(normalized_data, headers="keys", tablefmt="github")
    print(table3)

main()