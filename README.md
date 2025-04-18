# Customer Sales Analytics

## Overview
The **Customer Sales Analytics** project is designed to provide insights into sales data by analyzing customer behavior, orders, products, 
payments, and other relevant factors. This project leverages SQL, Python, and data visualization to help businesses make data-driven decisions 
and improve sales strategies.

## Project Description
The project involves building a comprehensive database that stores and analyzes customer sales information. The dataset includes customer 
information, product details, order history, payment transactions, shipping details, and more. Using various SQL queries, data transformations, 
and visualizations, key insights are extracted to optimize sales operations and strategies.

## Features
- **Customer Segmentation**: Classify customers based on their purchase behavior.
- **Order Fulfillment**: Analyze order delivery times and fulfillment speed.
- **Sales Trends**: Visualize trends and patterns in customer purchases.
- **Payment Analysis**: Evaluate payment methods, success rates, and amounts.
- **Product Insights**: Analyze top-selling products, product categories, and stock levels.

## Technologies Used
- **SQL**: For querying and manipulating data in the MySQL database.
- **Python**: For data processing, visualization, and analysis.
- **Pandas**: For data manipulation and cleaning.
- **MySQL**: For managing the relational database and storing sales data.
- **Matplotlib/Seaborn**: For generating visualizations and plots.

## How to Run the Project
1. **Clone the repository**:
    ```bash
    git clone https://github.com/glokol22/Customer_Sales_Analytics.git
    ```

2. **Install necessary packages**:
    Make sure to install the required libraries by running:
    ```bash
    pip install -r requirements.txt
    ```

3. **Set up your database**:
    Ensure that you have a MySQL server running locally or remotely, and create a database named `customer_sales_analytics`.

4. **Import data**:
    Import the CSV files in the project into your MySQL database using the provided SQL scripts or Python scripts.

5. **Run analysis**:
    Execute the Jupyter Notebooks (`query.ipynb`, `data_validation.ipynb`, `normalization.ipynb`) for running SQL queries, data analysis, and visualization.
