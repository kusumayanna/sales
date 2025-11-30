import os
import psycopg2
from psycopg2 import extras
import csv
from pathlib import Path
import time

from utils import get_db_url


STAGING_CREATE_SQL = """
-- Drop existing tables if they exist (in correct order due to foreign keys)
DROP TABLE IF EXISTS OrderDetail CASCADE;
DROP TABLE IF EXISTS Product CASCADE;
DROP TABLE IF EXISTS ProductCategory CASCADE;
DROP TABLE IF EXISTS Customer CASCADE;
DROP TABLE IF EXISTS Country CASCADE;
DROP TABLE IF EXISTS Region CASCADE;

-- Lookup/Dimension tables
CREATE TABLE Region (
    RegionID SERIAL PRIMARY KEY,
    Region TEXT NOT NULL UNIQUE
);

CREATE TABLE Country (
    CountryID SERIAL PRIMARY KEY,
    Country TEXT NOT NULL UNIQUE,
    RegionID INTEGER NOT NULL,
    FOREIGN KEY (RegionID) REFERENCES Region(RegionID)
);

CREATE TABLE ProductCategory (
    ProductCategoryID SERIAL PRIMARY KEY,
    ProductCategory TEXT NOT NULL UNIQUE,
    ProductCategoryDescription TEXT
);

-- Entity tables
CREATE TABLE Customer (
    CustomerID SERIAL PRIMARY KEY,
    FirstName TEXT NOT NULL,
    LastName TEXT NOT NULL,
    Address TEXT,
    City TEXT,
    CountryID INTEGER,
    FOREIGN KEY (CountryID) REFERENCES Country(CountryID)
);

CREATE TABLE Product (
    ProductID SERIAL PRIMARY KEY,
    ProductName TEXT NOT NULL UNIQUE,
    ProductUnitPrice REAL NOT NULL,
    ProductCategoryID INTEGER NOT NULL,
    FOREIGN KEY (ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID)
);

-- Fact table
CREATE TABLE OrderDetail (
    OrderID SERIAL PRIMARY KEY,
    CustomerID INTEGER NOT NULL,
    ProductID INTEGER NOT NULL,
    OrderDate DATE NOT NULL,
    QuantityOrdered INTEGER NOT NULL,
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES Product(ProductID)
);
"""

# Note: You'll need to provide the data file. Expected format is TSV with these columns:
FILES = {
    "orders": {
        "filename": "orders_data.txt",  # TSV file with all order data
        "batch_size": 5_000,
    }
}

EXPECTED_COLUMNS = [
    "Name",
    "Address", 
    "City",
    "Country",
    "Region",
    "ProductName",
    "ProductCategory",
    "ProductUnitPrice",
    "QuantityOrderded",  # Note: typo matches your original code
    "OrderDate"
]


def create_connection(db_url):
    """Create a database connection to PostgreSQL"""
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


def build_dimensions_from_data(conn, filepath):
    """
    Parse the data file and populate dimension tables:
    - Region
    - Country (with RegionID foreign key)
    - ProductCategory
    """
    path = Path(filepath)
    if not path.exists():
        print(f"Warning: Data file not found: {filepath}")
        return
    
    cur = conn.cursor()
    
    # Collect unique values
    regions = set()
    countries_regions = set()  # (Country, Region) pairs
    product_categories = {}  # ProductCategory -> Description
    
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            region = row.get('Region', '').strip()
            country = row.get('Country', '').strip()
            prod_cat = row.get('ProductCategory', '').strip()
            
            if region:
                regions.add(region)
            if country and region:
                countries_regions.add((country, region))
            
            # Handle multiple product categories (semicolon-separated)
            if prod_cat:
                for cat in prod_cat.split(';'):
                    cat = cat.strip()
                    if cat and cat not in product_categories:
                        product_categories[cat] = cat  # Using category as description
    
    # Insert Regions
    if regions:
        regions_list = [(r,) for r in sorted(regions)]
        extras.execute_batch(
            cur,
            "INSERT INTO Region(Region) VALUES (%s) ON CONFLICT (Region) DO NOTHING;",
            regions_list
        )
    
    # Get Region mapping
    cur.execute("SELECT RegionID, Region FROM Region;")
    region_map = {row[1]: row[0] for row in cur.fetchall()}
    
    # Insert Countries
    if countries_regions:
        countries_list = [(c, region_map.get(r)) for c, r in sorted(countries_regions) if region_map.get(r)]
        extras.execute_batch(
            cur,
            "INSERT INTO Country(Country, RegionID) VALUES (%s, %s) ON CONFLICT (Country) DO NOTHING;",
            countries_list
        )
    
    # Insert ProductCategories
    if product_categories:
        cat_list = [(cat, desc) for cat, desc in sorted(product_categories.items())]
        extras.execute_batch(
            cur,
            "INSERT INTO ProductCategory(ProductCategory, ProductCategoryDescription) VALUES (%s, %s) ON CONFLICT (ProductCategory) DO NOTHING;",
            cat_list
        )
    
    conn.commit()
    cur.close()
    print("Dimension tables populated (Region, Country, ProductCategory)")


def load_customers(conn, filepath):
    """
    Parse the data file and populate Customer table
    """
    path = Path(filepath)
    if not path.exists():
        print(f"Warning: Data file not found: {filepath}")
        return
    
    cur = conn.cursor()
    
    # Get Country mapping
    cur.execute("SELECT CountryID, Country FROM Country;")
    country_map = {row[1]: row[0] for row in cur.fetchall()}
    
    # Collect unique customers
    customers = {}  # (FirstName, LastName) -> (Address, City, Country)
    
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            name = row.get('Name', '').strip()
            address = row.get('Address', '').strip()
            city = row.get('City', '').strip()
            country = row.get('Country', '').strip()
            
            if name:
                # Split name into first and last
                parts = name.split(' ', 1)
                if len(parts) == 2:
                    first_name, last_name = parts
                else:
                    first_name = parts[0]
                    last_name = ''
                
                key = (first_name, last_name)
                if key not in customers:
                    customers[key] = (address, city, country)
    
    # Insert customers
    customer_list = []
    for (fname, lname), (addr, city, country) in sorted(customers.items()):
        country_id = country_map.get(country)
        customer_list.append((fname, lname, addr, city, country_id))
    
    if customer_list:
        extras.execute_batch(
            cur,
            "INSERT INTO Customer(FirstName, LastName, Address, City, CountryID) VALUES (%s, %s, %s, %s, %s);",
            customer_list
        )
    
    conn.commit()
    cur.close()
    print(f"Customer table populated with {len(customer_list)} customers")


def load_products(conn, filepath):
    """
    Parse the data file and populate Product table
    """
    path = Path(filepath)
    if not path.exists():
        print(f"Warning: Data file not found: {filepath}")
        return
    
    cur = conn.cursor()
    
    # Get ProductCategory mapping
    cur.execute("SELECT ProductCategoryID, ProductCategory FROM ProductCategory;")
    cat_map = {row[1]: row[0] for row in cur.fetchall()}
    
    # Collect unique products
    products = set()  # (ProductName, ProductUnitPrice, ProductCategoryID)
    
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            prod_names = row.get('ProductName', '').strip()
            prod_cats = row.get('ProductCategory', '').strip()
            prod_prices = row.get('ProductUnitPrice', '').strip()
            
            if not (prod_names and prod_cats and prod_prices):
                continue
            
            # Handle semicolon-separated values
            names = [p.strip() for p in prod_names.split(';')]
            cats = [c.strip() for c in prod_cats.split(';')]
            prices = [p.strip() for p in prod_prices.split(';')]
            
            for name, cat, price in zip(names, cats, prices):
                if name and cat and price:
                    try:
                        price_val = float(price)
                        cat_id = cat_map.get(cat)
                        if cat_id:
                            products.add((name, price_val, cat_id))
                    except ValueError:
                        continue
    
    # Insert products
    product_list = [(name, price, cat_id) for name, price, cat_id in sorted(products)]
    
    if product_list:
        extras.execute_batch(
            cur,
            "INSERT INTO Product(ProductName, ProductUnitPrice, ProductCategoryID) VALUES (%s, %s, %s) ON CONFLICT (ProductName) DO NOTHING;",
            product_list
        )
    
    conn.commit()
    cur.close()
    print(f"Product table populated with {len(product_list)} products")


def load_orders(conn, filepath):
    """
    Parse the data file and populate OrderDetail table
    """
    path = Path(filepath)
    if not path.exists():
        print(f"Warning: Data file not found: {filepath}")
        return
    
    cur = conn.cursor()
    
    # Get Customer mapping (FirstName LastName -> CustomerID)
    cur.execute("SELECT CustomerID, FirstName, LastName FROM Customer;")
    customer_map = {f"{row[1]} {row[2]}": row[0] for row in cur.fetchall()}
    
    # Get Product mapping
    cur.execute("SELECT ProductID, ProductName FROM Product;")
    product_map = {row[1]: row[0] for row in cur.fetchall()}
    
    # Collect orders
    orders = []
    
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            name = row.get('Name', '').strip()
            prod_names = row.get('ProductName', '').strip()
            quantities = row.get('QuantityOrderded', '').strip()  # Note: typo in original
            dates = row.get('OrderDate', '').strip()
            
            customer_id = customer_map.get(name)
            if not customer_id:
                continue
            
            if not (prod_names and quantities and dates):
                continue
            
            # Handle semicolon-separated values
            names_list = [p.strip() for p in prod_names.split(';')]
            qty_list = [q.strip() for q in quantities.split(';')]
            date_list = [d.strip() for d in dates.split(';')]
            
            for pname, qty, date in zip(names_list, qty_list, date_list):
                if pname and qty and date:
                    product_id = product_map.get(pname)
                    if not product_id:
                        continue
                    
                    try:
                        qty_val = int(qty)
                        # Convert date from YYYYMMDD to YYYY-MM-DD
                        if len(date) == 8:
                            formatted_date = f"{date[0:4]}-{date[4:6]}-{date[6:8]}"
                        else:
                            formatted_date = date
                        
                        orders.append((customer_id, product_id, formatted_date, qty_val))
                    except (ValueError, IndexError):
                        continue
    
    # Insert orders
    if orders:
        extras.execute_batch(
            cur,
            "INSERT INTO OrderDetail(CustomerID, ProductID, OrderDate, QuantityOrdered) VALUES (%s, %s, %s, %s);",
            orders
        )
    
    conn.commit()
    cur.close()
    print(f"OrderDetail table populated with {len(orders)} orders")


# SQL Query Functions
def ex1(conn, CustomerName):
    """
    Fetch all order details for a given CustomerName
    Returns: Name, ProductName, OrderDate, ProductUnitPrice, QuantityOrdered, Total
    """
    sql_statement = """
    SELECT 
        C.FirstName || ' ' || C.LastName AS Name,
        P.ProductName,
        O.OrderDate,
        P.ProductUnitPrice,
        O.QuantityOrdered,
        ROUND(CAST(P.ProductUnitPrice * O.QuantityOrdered AS NUMERIC), 2) AS Total
    FROM OrderDetail O
    JOIN Customer C ON O.CustomerID = C.CustomerID
    JOIN Product P ON O.ProductID = P.ProductID
    WHERE C.FirstName || ' ' || C.LastName = %s
    """
    cur = conn.cursor()
    cur.execute(sql_statement, (CustomerName,))
    return cur.fetchall()


def ex2(conn, CustomerName):
    """
    Sum total for a given CustomerName
    Returns: Name, Total
    """
    sql_statement = """
    SELECT
        (C.FirstName || ' ' || C.LastName) AS Name,
        ROUND(CAST(SUM(P.ProductUnitPrice * O.QuantityOrdered) AS NUMERIC), 2) AS Total
    FROM OrderDetail AS O
    JOIN Customer AS C ON O.CustomerID = C.CustomerID
    JOIN Product  AS P ON O.ProductID  = P.ProductID
    WHERE (C.FirstName || ' ' || C.LastName) = %s
    GROUP BY C.CustomerID, C.FirstName, C.LastName
    """
    cur = conn.cursor()
    cur.execute(sql_statement, (CustomerName,))
    return cur.fetchall()


def ex3(conn):
    """
    Find the total for all customers
    Returns: Name, Total (ordered by Total DESC)
    """
    sql_statement = """
    SELECT
        (C.FirstName || ' ' || C.LastName) AS Name,
        ROUND(CAST(SUM(P.ProductUnitPrice * O.QuantityOrdered) AS NUMERIC), 2) AS Total
    FROM OrderDetail O
    JOIN Customer C ON O.CustomerID = C.CustomerID
    JOIN Product  P ON O.ProductID  = P.ProductID
    GROUP BY C.CustomerID, C.FirstName, C.LastName
    ORDER BY Total DESC
    """
    cur = conn.cursor()
    cur.execute(sql_statement)
    return cur.fetchall()


def ex4(conn):
    """
    Find the total for all regions
    Returns: Region, Total (ordered by Total DESC)
    """
    sql_statement = """
    SELECT
        R.Region AS Region,
        ROUND(CAST(SUM(P.ProductUnitPrice * O.QuantityOrdered) AS NUMERIC), 2) AS Total
    FROM OrderDetail O
    JOIN Customer C ON O.CustomerID = C.CustomerID
    JOIN Product  P ON O.ProductID  = P.ProductID
    JOIN Country Y ON C.CountryID   = Y.CountryID
    JOIN Region  R ON Y.RegionID    = R.RegionID
    GROUP BY R.RegionID, R.Region
    ORDER BY Total DESC
    """
    cur = conn.cursor()
    cur.execute(sql_statement)
    return cur.fetchall()


def ex5(conn):
    """
    Find the total for all countries
    Returns: Country, Total (ordered by Total DESC)
    """
    sql_statement = """
    SELECT
        Y.Country AS Country,
        ROUND(CAST(SUM(P.ProductUnitPrice * O.QuantityOrdered) AS NUMERIC), 0) AS Total
    FROM OrderDetail O
    JOIN Customer C ON O.CustomerID = C.CustomerID
    JOIN Product  P ON O.ProductID  = P.ProductID
    JOIN Country Y ON C.CountryID   = Y.CountryID
    GROUP BY Y.CountryID, Y.Country
    ORDER BY Total DESC
    """
    cur = conn.cursor()
    cur.execute(sql_statement)
    return cur.fetchall()


def ex6(conn):
    """
    Rank the countries within a region based on order total
    Returns: Region, Country, CountryTotal, TotalRank
    """
    sql_statement = """
    SELECT 
        R.Region,
        Y.Country,
        ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) AS CountryTotal,
        DENSE_RANK() OVER (PARTITION BY R.Region ORDER BY SUM(P.ProductUnitPrice * O.QuantityOrdered) DESC) AS TotalRank
    FROM OrderDetail O
    JOIN Customer C ON O.CustomerID = C.CustomerID
    JOIN Product P ON O.ProductID = P.ProductID
    JOIN Country Y ON C.CountryID = Y.CountryID
    JOIN Region R ON Y.RegionID = R.RegionID
    GROUP BY R.Region, Y.Country
    ORDER BY R.Region ASC, CountryTotal DESC
    """
    cur = conn.cursor()
    cur.execute(sql_statement)
    return cur.fetchall()


def ex7(conn):
    """
    Rank countries within region, but only select the TOP country per region
    Returns: Region, Country, CountryTotal, CountryRegionalRank
    """
    sql_statement = """
    WITH CountryStats AS (
        SELECT 
            R.Region,
            Y.Country,
            ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) AS CountryTotal,
            DENSE_RANK() OVER (PARTITION BY R.Region ORDER BY SUM(P.ProductUnitPrice * O.QuantityOrdered) DESC) AS CountryRegionalRank
        FROM OrderDetail O
        JOIN Customer C ON O.CustomerID = C.CustomerID
        JOIN Product P ON O.ProductID = P.ProductID
        JOIN Country Y ON C.CountryID = Y.CountryID
        JOIN Region R ON Y.RegionID = R.RegionID
        GROUP BY R.Region, Y.Country
    )
    SELECT 
        Region,
        Country,
        CountryTotal,
        CountryRegionalRank
    FROM CountryStats
    WHERE CountryRegionalRank = 1
    ORDER BY Region ASC
    """
    cur = conn.cursor()
    cur.execute(sql_statement)
    return cur.fetchall()


def ex8(conn):
    """
    Sum customer sales by Quarter and year
    Returns: Quarter, Year, CustomerID, Total
    """
    sql_statement = """
    SELECT 
        'Q' || EXTRACT(QUARTER FROM O.OrderDate)::TEXT AS Quarter,
        EXTRACT(YEAR FROM O.OrderDate)::INTEGER AS Year,
        O.CustomerID,
        ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) AS Total
    FROM OrderDetail O
    JOIN Product P ON O.ProductID = P.ProductID
    GROUP BY 
        EXTRACT(QUARTER FROM O.OrderDate),
        EXTRACT(YEAR FROM O.OrderDate),
        O.CustomerID
    ORDER BY Year ASC, Quarter ASC, O.CustomerID ASC
    """
    cur = conn.cursor()
    cur.execute(sql_statement)
    return cur.fetchall()


def ex9(conn):
    """
    Rank customer sales by Quarter and year, select top 5 customers per quarter
    Returns: Quarter, Year, CustomerID, Total, CustomerRank
    """
    sql_statement = """
    WITH CustomerSales AS (
        SELECT 
            'Q' || EXTRACT(QUARTER FROM O.OrderDate)::TEXT AS Quarter,
            EXTRACT(YEAR FROM O.OrderDate)::INTEGER AS Year,
            O.CustomerID,
            ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) AS Total
        FROM OrderDetail O
        JOIN Product P ON O.ProductID = P.ProductID
        GROUP BY 
            EXTRACT(QUARTER FROM O.OrderDate),
            EXTRACT(YEAR FROM O.OrderDate),
            O.CustomerID
    ),
    RankedSales AS (
        SELECT 
            Quarter,
            Year,
            CustomerID,
            Total,
            DENSE_RANK() OVER (PARTITION BY Quarter, Year ORDER BY Total DESC) AS CustomerRank
        FROM CustomerSales
    )
    SELECT 
        Quarter,
        Year,
        CustomerID,
        Total,
        CustomerRank
    FROM RankedSales
    WHERE CustomerRank <= 5
    ORDER BY Year ASC, Quarter ASC, Total DESC
    """
    cur = conn.cursor()
    cur.execute(sql_statement)
    return cur.fetchall()


def ex10(conn):
    """
    Rank the monthly sales
    Returns: Month, Total, TotalRank
    """
    sql_statement = """
    WITH Monthly_Sales_Data AS (
        SELECT 
            EXTRACT(MONTH FROM ord.OrderDate)::INTEGER AS Month_Index,
            SUM(ROUND(prod.ProductUnitPrice * ord.QuantityOrdered)) AS Raw_Total
        FROM OrderDetail ord
        INNER JOIN Product prod ON ord.ProductID = prod.ProductID
        GROUP BY EXTRACT(MONTH FROM ord.OrderDate)
    )
    SELECT 
        CASE Month_Index
            WHEN 1 THEN 'January'
            WHEN 2 THEN 'February'
            WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'
            WHEN 8 THEN 'August'
            WHEN 9 THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END AS Month,
        CAST(Raw_Total AS FLOAT) AS Total,
        RANK() OVER (ORDER BY Raw_Total DESC) AS TotalRank
    FROM Monthly_Sales_Data
    ORDER BY Total DESC
    """
    cur = conn.cursor()
    cur.execute(sql_statement)
    return cur.fetchall()


def ex11(conn):
    """
    Find the MaxDaysWithoutOrder for each customer
    Returns: CustomerID, FirstName, LastName, Country, OrderDate, PreviousOrderDate, MaxDaysWithoutOrder
    """
    sql_statement = """
    WITH OrderedOrders AS (
        SELECT 
            O.CustomerID,
            O.OrderDate,
            LAG(O.OrderDate, 1) OVER (PARTITION BY O.CustomerID ORDER BY O.OrderDate) AS PreviousOrderDate
        FROM OrderDetail O
    ),
    Gaps AS (
        SELECT 
            CustomerID,
            OrderDate,
            PreviousOrderDate,
            (OrderDate - PreviousOrderDate) AS DaysWithoutOrder
        FROM OrderedOrders
        WHERE PreviousOrderDate IS NOT NULL
    ),
    MaxGaps AS (
        SELECT 
            CustomerID,
            OrderDate,
            PreviousOrderDate,
            DaysWithoutOrder,
            ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY DaysWithoutOrder DESC, OrderDate ASC) AS GapRank
        FROM Gaps
    )
    SELECT 
        M.CustomerID,
        C.FirstName,
        C.LastName,
        Y.Country,
        M.OrderDate,
        M.PreviousOrderDate,
        M.DaysWithoutOrder AS MaxDaysWithoutOrder
    FROM MaxGaps M
    JOIN Customer C ON M.CustomerID = C.CustomerID
    JOIN Country Y ON C.CountryID = Y.CountryID
    WHERE M.GapRank = 1
    ORDER BY MaxDaysWithoutOrder DESC, M.CustomerID DESC
    """
    cur = conn.cursor()
    cur.execute(sql_statement)
    return cur.fetchall()


# Main execution
if __name__ == "__main__":
    
    DATABASE_URL = get_db_url()
    
    # Create tables
    print("Creating tables...")
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute(STAGING_CREATE_SQL)
    conn.commit()
    cursor.close()
    conn.close()
    print("Tables created successfully\n")

    # Check if data file exists
    data_file = FILES["orders"]["filename"]
    if not Path(data_file).exists():
        print(f"\n⚠️  WARNING: Data file '{data_file}' not found!")
        print("Please provide a TSV file with the following columns:")
        print(", ".join(EXPECTED_COLUMNS))
        print("\nDatabase schema has been created, but no data has been loaded.")
    else:
        # Build dimensions
        print("Building dimension tables...")
        conn = psycopg2.connect(DATABASE_URL)
        build_dimensions_from_data(conn, data_file)
        conn.close()

        # Load customers
        print("Loading customers...")
        conn = psycopg2.connect(DATABASE_URL)
        load_customers(conn, data_file)
        conn.close()

        # Load products
        print("Loading products...")
        conn = psycopg2.connect(DATABASE_URL)
        load_products(conn, data_file)
        conn.close()

        # Load orders
        print("Loading orders...")
        conn = psycopg2.connect(DATABASE_URL)
        load_orders(conn, data_file)
        conn.close()
        
        print("\n Database migration complete!")
