import psycopg2
import pandas as pd

# Database connection details
DB_NAME = "Certainti"
DB_USER = "postgres"
DB_PASSWORD = "12345"
DB_HOST = "localhost"
DB_PORT = "5432"


# Function to connect to the database
def connect_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("✅ Database connected successfully!")
        return conn
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return None
        
# -- Task 1: Schema Creation 

# --Part 1: Database Design & Schema Creation


def create_tables(conn):
    try:
        cur = conn.cursor()

        # Creating the tables (Using IF NOT EXISTS to avoid errors)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stores (
                store_id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                location VARCHAR(255) NOT NULL,
                manager_id INT
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                employee_id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                role VARCHAR(100) NOT NULL,
                store_id INT REFERENCES stores(store_id) ON DELETE SET NULL,
                salary DECIMAL(10,2) CHECK (salary >= 0),
                manager_id INT REFERENCES employees(employee_id) ON DELETE SET NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                phone VARCHAR(20) UNIQUE NOT NULL,
                city VARCHAR(255) NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS suppliers (
                supplier_id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                contact_person VARCHAR(255) NOT NULL,
                phone VARCHAR(20) UNIQUE NOT NULL,
                city VARCHAR(255) NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                category VARCHAR(100) NOT NULL,
                price DECIMAL(10,2) CHECK (price >= 0),
                stock INT CHECK (stock >= 0),
                supplier_id INT REFERENCES suppliers(supplier_id) ON DELETE CASCADE
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id INT PRIMARY KEY,
                customer_id INT REFERENCES customers(customer_id) ON DELETE CASCADE,
                store_id INT REFERENCES stores(store_id) ON DELETE SET NULL,
                order_date DATE NOT NULL DEFAULT CURRENT_DATE,
                total_amount DECIMAL(10,2) CHECK (total_amount >= 0)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS order_items (
                order_item_id INT PRIMARY KEY,
                order_id INT REFERENCES orders(order_id) ON DELETE CASCADE,
                product_id INT REFERENCES products(product_id) ON DELETE CASCADE,
                quantity INT CHECK (quantity > 0),
                price DECIMAL(10,2) CHECK (price >= 0)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                payment_id INT PRIMARY KEY,
                order_id INT REFERENCES orders(order_id) ON DELETE CASCADE,
                amount DECIMAL(10,2) CHECK (amount >= 0),
                payment_method VARCHAR(50) NOT NULL,
                payment_date DATE NOT NULL DEFAULT CURRENT_DATE
            );
        """)

        conn.commit()
        cur.close()
        print("✅ Tables created successfully (if not already existing)!")

    except Exception as e:
        print(f"❌ Error creating tables: {e}")
        
 
                                           
 # -- Part 2: Indexing for Performance Optimization 

# -- Task 2: Create Indexes 

def create_indexes(conn):
    try:
        cur = conn.cursor()

        # Index on product name for fast searching
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_product_name
            ON products (name);
        """)

        # Composite index on (customer_id, order_date) for faster order retrieval
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_customer_order_date
            ON orders (customer_id, order_date);
        """)

        conn.commit()
        cur.close()
        print("✅ Indexes created successfully!")

    except Exception as e:
        print(f"❌ Error creating indexes: {e}")


# -- Part 3: Views and Triggers 

# -- Task 3: Create Views

def create_views(conn):
    try:
        cur = conn.cursor()

        # View for top-selling products based on total quantity sold
        cur.execute("""
            CREATE OR REPLACE VIEW top_selling_products AS
            SELECT 
                p.product_id,
                p.name AS product_name,
                SUM(oi.quantity) AS total_quantity_sold
            FROM order_items oi
            JOIN products p ON oi.product_id = p.product_id
            GROUP BY p.product_id, p.name
            ORDER BY total_quantity_sold DESC;
        """)

        # View for total revenue per store
        cur.execute("""
            CREATE OR REPLACE VIEW store_revenue AS
            SELECT 
                s.store_id,
                s.name AS store_name,
                COALESCE(SUM(o.total_amount), 0) AS total_revenue
            FROM stores s
            LEFT JOIN orders o ON s.store_id = o.store_id
            GROUP BY s.store_id, s.name
            ORDER BY total_revenue DESC;
        """)

        conn.commit()
        cur.close()
        print("✅ Views created successfully!")

    except Exception as e:
        print(f"❌ Error creating views: {e}") 
        
# -- Task 4: Create Triggers 

#  -- trigger to prevent orders for out-of-stock products.    

def create_triggers(conn):
    try:
        cur = conn.cursor()

        # 1️⃣ Trigger to prevent orders for out-of-stock products
        cur.execute("""
            CREATE OR REPLACE FUNCTION prevent_out_of_stock_orders()
            RETURNS TRIGGER AS $$
            BEGIN
                -- Check if there is enough stock
                IF (SELECT stock FROM products WHERE product_id = NEW.product_id) < NEW.quantity THEN
                    RAISE EXCEPTION 'Cannot place order: Not enough stock for product ID %', NEW.product_id;
                END IF;

                -- Reduce stock after successful order placement
                UPDATE products
                SET stock = stock - NEW.quantity
                WHERE product_id = NEW.product_id;

                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)

        cur.execute("""
            CREATE OR REPLACE TRIGGER check_stock_before_order
            BEFORE INSERT ON order_items
            FOR EACH ROW
            EXECUTE FUNCTION prevent_out_of_stock_orders();
        """)

        # 2️⃣ Create an audit table for deleted employees
        cur.execute("""
            CREATE TABLE IF NOT EXISTS employee_audit (
                audit_id SERIAL PRIMARY KEY,
                employee_id INT,
                name VARCHAR(255),
                role VARCHAR(255),
                store_id INT,
                salary DECIMAL(10,2),
                manager_id INT,
                deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # 3️⃣ Trigger to log deleted employees before deletion
        cur.execute("""
            CREATE OR REPLACE FUNCTION log_deleted_employee()
            RETURNS TRIGGER AS $$
            BEGIN
                INSERT INTO employee_audit (employee_id, name, role, store_id, salary, manager_id)
                VALUES (OLD.employee_id, OLD.name, OLD.role, OLD.store_id, OLD.salary, OLD.manager_id);
                RETURN OLD;
            END;
            $$ LANGUAGE plpgsql;
        """)

        cur.execute("""
            CREATE OR REPLACE TRIGGER log_employee_deletion
            BEFORE DELETE ON employees
            FOR EACH ROW
            EXECUTE FUNCTION log_deleted_employee();
        """)

        conn.commit()
        cur.close()
        print("✅ Triggers created successfully!")

    except Exception as e:
        print(f"❌ Error creating triggers: {e}")
        
# -- Part 4: Data Insertion & Importing from Files 
                                        
#   Task 5: Insert Data Manually 
                                    
def insert_sample_data(conn):
    try:
        cur = conn.cursor()

        # Insert Stores
        cur.execute("""
            INSERT INTO stores (store_id, name, location, manager_id) VALUES
            (1, 'ShopEase Mart - Chennai', 'Chennai, Tamil Nadu', 101),
            (2, 'ShopEase Mart - Mumbai', 'Mumbai, Maharashtra', 102),
            (3, 'ShopEase Mart - Bengaluru', 'Bengaluru, Karnataka', 103),
            (4, 'ShopEase Mart - Delhi', 'New Delhi, Delhi', 104),
            (5, 'ShopEase Mart - Hyderabad', 'Hyderabad, Telangana', 105)
            
            ON CONFLICT (store_id) DO NOTHING;
        """)


# Insert new Stores
cur.execute("""
    INSERT INTO stores (store_id, name, location, manager_id) VALUES
    (1, 'MegaMart - Chennai', 'Chennai, Tamil Nadu', 201),
    (2, 'MegaMart - Mumbai', 'Mumbai, Maharashtra', 202),
    (3, 'MegaMart - Bengaluru', 'Bengaluru, Karnataka', 203),
    (4, 'MegaMart - Delhi', 'New Delhi, Delhi', 204),
    (5, 'MegaMart - Hyderabad', 'Hyderabad, Telangana', 205)
    ON CONFLICT (store_id) DO NOTHING;
""")

# Insert new Employees
cur.execute("""
    INSERT INTO employees (employee_id, name, role, store_id, salary, manager_id) VALUES
    (201, 'Anil Sharma', 'Manager', 1, 80000, NULL),
    (202, 'Sunita Patel', 'Manager', 2, 78000, NULL),
    (203, 'Vijay Kumar', 'Manager', 3, 75000, NULL),
    (204, 'Ritu Singh', 'Manager', 4, 72000, NULL),
    (205, 'Karthik Nair', 'Manager', 5, 70000, NULL),
    (206, 'Suman Reddy', 'Cashier', 1, 38000, 201),
    (207, 'Lata Joshi', 'Cashier', 2, 36000, 202),
    (208, 'Rohan Menon', 'Sales Associate', 3, 34000, 203),
    (209, 'Divya Rao', 'Sales Associate', 4, 33000, 204),
    (210, 'Ajay Gupta', 'Stock Clerk', 5, 32000, 205)
    ON CONFLICT (employee_id) DO NOTHING;
""")

# Insert new Suppliers (10)
cur.execute("""
    INSERT INTO suppliers (supplier_id, name, contact_person, phone, city) VALUES
    (1, 'Green Fields', 'Suresh Menon', '9988776655', 'Chennai'),
    (2, 'Tech Innovations', 'Riya Kapoor', '9876543211', 'Mumbai'),
    (3, 'Pure Foods', 'Vikram Shah', '8765432108', 'Delhi'),
    (4, 'Style Suppliers', 'Anita Desai', '7654321097', 'Hyderabad'),
    (5, 'Home Essentials', 'Rajesh Patil', '6543210986', 'Bengaluru'),
    (6, 'Tech Trends', 'Mohan Das', '9532109876', 'Hyderabad'),
    (7, 'Fashion Hub', 'Sita Iyer', '9421098765', 'Pune'),
    (8, 'Grocery Supplies', 'Arun Gupta', '9310987654', 'Ahmedabad'),
    (9, 'Home Decor Ltd', 'Priya Shah', '9209876543', 'Jaipur'),
    (10, 'Fitness Gear', 'Vikas Menon', '9198765432', 'Lucknow')
    ON CONFLICT (supplier_id) DO NOTHING;
""")

# Insert new Customers (20)
cur.execute("""
    INSERT INTO customers (customer_id, name, email, phone, city) VALUES
    (1, 'Rekha Pillai', 'rekha.pillai@example.com', '9123456789', 'Chennai'),
    (2, 'Sanjay Kapoor', 'sanjay.kapoor@example.com', '9234567890', 'Mumbai'),
    (3, 'Lakshmi Shah', 'lakshmi.shah@example.com', '9345678901', 'Delhi'),
    (4, 'Kiran Menon', 'kiran.menon@example.com', '9456789012', 'Hyderabad'),
    (5, 'Rohit Malhotra', 'rohit.malhotra@example.com', '9567890123', 'Bengaluru'),
    (6, 'Suman Reddy', 'suman@example.com', '9988776648', 'Hyderabad'),
    (7, 'Naveen Joshi', 'naveen@example.com', '9876543204', 'Pune'),
    (8, 'Rita Singh', 'rita@example.com', '9988776649', 'Ahmedabad'),
    (9, 'Mahesh Patel', 'mahesh@example.com', '9876543205', 'Jaipur'),
    (10, 'Priya Desai', 'priya@example.com', '9988776650', 'Lucknow'),
    (11, 'Vikas Men Justicia', 'vikas@example.com', '9876543206', 'Chandigarh'),
    (12, 'Deepa Joshi', 'deepa@example.com', '9988776651', 'Bhopal'),
    (13, 'Sita Iyer', 'sita@example.com', '9876543207', 'Indore'),
    (14, 'Arun Gupta', 'arun@example.com', '9988776652', 'Surat'),
    (15, 'Manju Gupta', 'manju@example.com', '9876543208', 'Vadodara'),
    (16, 'Kavya Iyer', 'kavya@example.com', '9988776653', 'Coimbatore'),
    (17, 'Rekha Menon', 'rekha2@example.com', '9876543209', 'Visakhapatnam'),
    (18, 'Mohan Sharma', 'mohan@example.com', '9988776654', 'Nagpur'),
    (19, 'Nita Desai', 'nita@example.com', '9876543210', 'Patna'),
    (20, 'Vijay Rao', 'vijay@example.com', '8111111111', 'Hyderabad')
    ON CONFLICT (customer_id) DO NOTHING;
""")

# Insert new Products (50)
cur.execute("""
    INSERT INTO products (product_id, name, category, price, stock, supplier_id) VALUES
    (1, 'Premium Rice', 'Groceries', 90.00, 250, 1),
    (2, 'Bluetooth Earphones', 'Electronics', 2699.00, 60, 2),
    (3, 'Multigrain Flour', 'Groceries', 60.00, 180, 3),
    (4, 'Sports Shoes', 'Sports', 3199.00, 80, 4),
    (5, 'Herbal Shampoo', 'Personal Care', 219.00, 130, 5),
    (6, 'Leather Purse', 'Accessories', 999.00, 70, 1),
    (7, 'Desk Lamp', 'Home Supplies', 899.00, 90, 2),
    (8, 'Chai Blend', 'Beverages', 170.00, 350, 3),
    (9, 'Aloe Vera Face Wash', 'Personal Care', 199.00, 270, 4),
    (10, 'Tennis Racket', 'Sports', 5299.00, 100, 5),
    (11, 'Smart Watch', 'Electronics', 1699.00, 50, 1),
    (12, 'Herbal Tea', 'Beverages', 270.00, 200, 2),
    (13, 'Herbal Toothpaste', 'Personal Care', 109.00, 550, 3),
    (14, 'Portable Speaker', 'Electronics', 3199.00, 80, 4),
    (15, 'Liquid Detergent', 'Home Supplies', 369.00, 450, 5),
    (16, 'Travel Backpack', 'Accessories', 2399.00, 60, 1),
    (17, 'Phone Case', 'Electronics', 319.00, 130, 2),
    (18, 'Filter Coffee', 'Beverages', 519.00, 220, 3),
    (19, 'Face Cream', 'Personal Care', 470.00, 160, 4),
    (20, 'Exercise Mat', 'Fitness', 999.00, 100, 5),
    (21, 'Convection Oven', 'Home Appliances', 13999.00, 35, 1),
    (22, 'Smart TV 50 Inch', 'Electronics', 26999.00, 25, 2),
    (23, 'Immersion Blender', 'Home Appliances', 1599.00, 55, 3),
    (24, 'Hair Straightener', 'Personal Care', 1399.00, 110, 4),
    (25, 'Optical Mouse', 'Computers', 1099.00, 90, 5),
    (26, 'Mechanical Keyboard', 'Computers', 3699.00, 50, 1),
    (27, 'Journal', 'Stationery', 90.00, 650, 2),
    (28, 'Gel Pen', 'Stationery', 25.00, 1100, 3),
    (29, 'Sanitizer Spray', 'Personal Care', 109.00, 320, 4),
    (30, 'Polarized Sunglasses', 'Accessories', 1599.00, 100, 5),
    (31, 'Robot Vacuum', 'Home Appliances', 8499.00, 30, 1),
    (32, 'Electric Pot', 'Kitchen', 1399.00, 65, 2),
    (33, 'Pressure Cooker', 'Kitchen', 2699.00, 45, 3),
    (34, 'Water Heater', 'Home Appliances', 7999.00, 25, 4),
    (35, 'RO Purifier', 'Home Appliances', 11999.00, 20, 5),
    (36, 'Air Freshener', 'Automobile', 519.00, 130, 1),
    (37, 'Motorcycle Helmet', 'Automobile', 2499.00, 85, 2),
    (38, 'Ergonomic Chair', 'Furniture', 7499.00, 40, 3),
    (39, 'Wooden Table', 'Furniture', 16999.00, 15, 4),
    (40, 'Table Lamp', 'Home Decor', 3499.00, 45, 5),
    (41, 'Wall Art', 'Home Decor', 2699.00, 35, 1),
    (42, 'Mirrorless Camera', 'Electronics', 20999.00, 20, 2),
    (43, 'Travel Suitcase', 'Travel', 6199.00, 55, 3),
    (44, 'Trekking Shoes', 'Footwear', 4199.00, 70, 4),
    (45, 'Kitchen Set', 'Kitchen', 4799.00, 45, 5),
    (46, 'Work Desk', 'Furniture', 9499.00, 30, 1),
    (47, 'Wall Shelf', 'Furniture', 5199.00, 35, 2),
    (48, 'Home Automation Kit', 'Electronics', 13999.00, 25, 3),
    (49, 'ANC Headphones', 'Electronics', 9499.00, 40, 4),
    (50, 'Decorative Plants', 'Home Decor', 849.00, 80, 5)
    ON CONFLICT (product_id) DO NOTHING;
""")

# Insert new Orders (100)
cur.execute("""
    INSERT INTO orders (order_id, customer_id, store_id, order_date, total_amount) VALUES
    (1, 1, 1, '2023-04-01', 450.00),
    (2, 2, 2, '2021-08-15', 849.00),
    (3, 3, 3, '2025-02-10', 1300.00),
    (4, 4, 4, '2022-10-25', 1699.00),
    (5, 5, 5, '2024-06-30', 2600.00),
    (6, 6, 1, '2022-12-03', 3200.00),
    (7, 7, 2, '2021-09-19', 900.00),
    (8, 8, 3, '2023-05-12', 1350.00),
    (9, 9, 4, '2025-07-18', 5000.00),
    (10, 10, 5, '2024-01-05', 2850.00),
    (11, 11, 1, '2021-04-20', 650.00),
    (12, 12, 2, '2023-11-08', 1550.00),
    (13, 13, 3, '2022-08-22', 3700.00),
    (14, 14, 4, '2025-03-17', 5100.00),
    (15, 15, 5, '2024-10-14', 830.00),
    (16, 16, 1, '2021-07-26', 2200.00),
    (17, 17, 2, '2024-01-28', 3350.00),
    (18, 18, 3, '2022-06-11', 3000.00),
    (19, 19, 4, '2025-09-07', 4300.00),
    (20, 20, 5, '2024-12-30', 3200.00),
    (21, 1, 1, '2022-03-03', 1600.00),
    (22, 2, 2, '2023-10-21', 2500.00),
    (23, 3, 3, '2025-05-05', 4800.00),
    (24, 4, 4, '2024-07-29', 1200.00),
    (25, 5, 5, '2022-01-15', 1900.00),
    (26, 6, 1, '2023-08-07', 2700.00),
    (27, 7, 2, '2022-04-23', 1000.00),
    (28, 8, 3, '2025-11-10', 3800.00),
    (29, 9, 4, '2024-02-25', 1600.00),
    (30, 10, 5, '2021-12-11', 4900.00),
    (31, 11, 1, '2023-03-19', 3300.00),
    (32, 2, 2, '2022-07-14', 5500.00),
    (33, 13, 3, '2025-08-30', 1450.00),
    (34, 14, 4, '2024-05-20', 2900.00),
    (35, 15, 5, '2021-06-09', 4400.00),
    (36, 16, 1, '2023-02-06', 550.00),
    (37, 17, 2, '2022-09-18', 6100.00),
    (38, 18, 3, '2025-10-03', 4200.00),
    (39, 19, 4, '2024-11-22', 1500.00),
    (40, 20, 5, '2021-08-27', 2850.00),
    (41, 1, 1, '2023-12-15', 3750.00),
    (42, 2, 2, '2022-05-28', 1650.00),
    (43, 3, 3, '2025-07-08', 2350.00),
    (44, 4, 4, '2024-04-19', 5000.00),
    (45, 5, 5, '2021-10-05', 750.00),
    (46, 6, 1, '2023-06-30', 8600.00),
    (47, 7, 2, '2023-01-10', 1400.00),
    (48, 8, 3, '2025-03-27', 3300.00),
    (49, 9, 4, '2024-08-16', 5800.00),
    (50, 10, 5, '2021-11-09', 1040.00),
    (51, 11, 1, '2023-09-25', 4600.00),
    (52, 12, 2, '2022-10-09', 3400.00),
    (53, 13, 3, '2025-04-13', 7600.00),
    (54, 14, 4, '2024-03-07', 2100.00),
    (55, 15, 5, '2021-07-04', 5200.00),
    (56, 16, 1, '2023-08-20', 1250.00),
    (57, 17, 2, '2022-12-04', 2850.00),
    (58, 18, 3, '2025-06-23', 6900.00),
    (59, 19, 4, '2024-09-11', 2600.00),
    (60, 20, 5, '2021-05-30', 3300.00),
    (61, 1, 1, '2023-07-06', 540.00),
    (62, 2, 2, '2022-11-14', 3200.00),
    (63, 3, 3, '2025-02-19', 4300.00),
    (64, 4, 4, '2024-06-02', 1400.00),
    (65, 5, 5, '2022-01-23', 3050.00),
    (66, 6, 1, '2023-04-16', 3800.00),
    (67, 7, 2, '2022-09-02', 2200.00),
    (68, 8, 3, '2025-08-28', 1200.00),
    (69, 9, 4, '2024-12-09', 3000.00),
    (70, 10, 5, '2021-10-18', 4350.00),
    (71, 11, 1, '2023-11-30', 6300.00),
    (72, 12, 2, '2022-04-05', 2850.00),
    (73, 13, 3, '2025-07-14', 600.00),
    (74, 14, 4, '2024-05-26', 4600.00),
    (75, 15, 5, '2021-08-11', 7300.00),
    (76, 16, 1, '2023-02-28', 2400.00),
    (77, 17, 2, '2022-06-17', 4900.00),
    (78, 18, 3, '2025-10-26', 1600.00),
    (79, 19, 4, '2024-03-12', 3500.00),
    (80, 20, 5, '2021-09-21', 3000.00),
    (81, 1, 1, '2024-01-03', 6100.00),
    (82, 2, 2, '2022-05-07', 1800.00),
    (83, 3, 3, '2025-06-15', 4200.00),
    (84, 4, 4, '2024-11-29', 2800.00),
    (85, 5, 5, '2021-12-20', 4400.00),
    (86, 6, 1, '2023-03-11', 3100.00),
    (87, 7, 2, '2022-10-28', 2600.00),
    (88, 8, 3, '2025-04-22', 5600.00),
    (89, 9, 4, '2024-02-29', 1000.00),
    (90, 10, 5, '2021-07-09', 1950.00),
    (91, 11, 1, '2023-08-14', 3350.00),
    (92, 12, 2, '2022-11-23', 2200.00),
    (93, 13, 3, '2025-02-08', 5500.00),
    (94, 14, 4, '2024-06-27', 1450.00),
    (95, 15, 5, '2021-09-04', 2800.00),
    (96, 16, 1, '2023-10-17', 4700.00),
    (97, 17, 2, '2022-07-11', 1600.00),
    (98, 18, 3, '2025-12-02', 3200.00),
    (99, 19, 4, '2024-12-13', 6300.00),
    (100, 20, 5, '2021-05-15', 5000.00)
    ON CONFLICT (order_id) DO NOTHING;
""")
        conn.commit()
        cur.close()
        print("✅ Sample data inserted successfully!")

    except Exception as e:
        print(f"❌ Error inserting sample data: {e}")
        
# Task 6: Import Data from CSV/XLSX

def load_xlsx_to_db(file_path, table_name, conn):
    cursor = conn.cursor()
    try:
        df = pd.read_excel(file_path)
        for _, row in df.iterrows():
            columns = ', '.join(df.columns)
            values = ', '.join(['%s'] * len(row))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            cursor.execute(insert_query, tuple(row))
        conn.commit()
        print(f"Data loaded successfully into {table_name} from {file_path}")
    except Exception as e:
        print(f"Error loading XLSX: {e}")
    finally:
        cursor.close()


# Part 5: Advanced Querying 

                            
# Task 7: Recursive Query (Common Table Expressions - CTE) 

def display_employee_hierarchy(conn):
    """
    Displays the hierarchical reporting structure of employees using a recursive CTE.
    """
    try:
        cur = conn.cursor()

        cur.execute("""
            WITH RECURSIVE EmployeeHierarchy AS (
                -- Anchor member: Select the CEO (manager_id is NULL)
                SELECT employee_id, name, role, manager_id, 0 AS level
                FROM Employees
                WHERE manager_id IS NULL

                UNION ALL

                -- Recursive member: Join with Employees to find subordinates
                SELECT e.employee_id, e.name, e.role, e.manager_id, eh.level + 1 AS level
                FROM Employees e
                JOIN EmployeeHierarchy eh ON e.manager_id = eh.employee_id
            )
            SELECT *
            FROM EmployeeHierarchy
            ORDER BY level, employee_id;
        """)

        results = cur.fetchall()

        if not results:
            print("No employees found or hierarchy could not be determined.")
            return

        print("\nEmployee Hierarchy:")
        print("--------------------")
        for row in results:
            employee_id, name, role, manager_id, level = row # unpack all columns
            indent = "  " * level  # Indentation for hierarchy level
            print(f"{indent}Level {level}: Employee ID: {employee_id}, Name: {name}, Role: {role}, Manager ID: {manager_id}") # Print all columns

        cur.close()

    except Exception as e:
        print(f"❌ Error displaying employee hierarchy: {e}")
    finally:
        if cur:
            cur.close()
            
 # Task 8: Pivot (Transpose Data) 


def display_monthly_sales_pivot_crosstab(conn):
    """
    Displays monthly sales data per store in pivot table format using PostgreSQL's crosstab function.
    """
    try:
        cur = conn.cursor()

        # --- Setup: Create extension and table if they don't exist ---
        cur.execute("CREATE EXTENSION IF NOT EXISTS tablefunc;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS monthly_sales (
                store_id INT REFERENCES stores(store_id),
                month TEXT,
                total_sales DECIMAL(10,2)
            );
        """)
        conn.commit()

        cur.execute("DELETE FROM monthly_sales;") 
        cur.executemany("""
            INSERT INTO monthly_sales (store_id, month, total_sales) VALUES (%s, %s, %s);
        """, [
            (1, 'Jan', 5000.00), (1, 'Feb', 7000.00), (1, 'Mar', 8000.00),
            (2, 'Jan', 6000.00), (2, 'Feb', 7500.00), (2, 'Mar', 9000.00)
        ])
        conn.commit() # Commit inserted data

        
        cur.execute("""
            SELECT * FROM crosstab(
                'SELECT store_id, month, total_sales FROM monthly_sales ORDER BY store_id, month',
                'SELECT DISTINCT month FROM monthly_sales ORDER BY month'
            )
            AS pivot_table (
                store_id INT, Jan DECIMAL(10,2), Feb DECIMAL(10,2), Mar DECIMAL(10,2)
            );
        """)

        pivot_results = cur.fetchall()

        if not pivot_results:
            print("No pivoted sales data to display.")
            return

        # --- Print the pivoted table ---
        print("\nMonthly Sales per Store (Pivot Table using crosstab):")
        print("----------------------------------------------------")

        # Header row
        header_row = ["Store ID", "Jan", "Feb", "Mar"]
        print("| " + " | ".join(header_row) + " |")
        print("-" * (len(header_row) * 12 + 5)) # Adjust separator length

        # Data rows
        for row in pivot_results:
            store_id, jan_sales, feb_sales, mar_sales = row
            data_row = [str(store_id), f"{jan_sales:.2f}" if jan_sales else '0.00',
                        f"{feb_sales:.2f}" if feb_sales else '0.00',
                        f"{mar_sales:.2f}" if mar_sales else '0.00'] # Handle NULL sales and format
            print("| " + " | ".join(data_row) + " |")

        cur.close()

    except Exception as e:
        print(f"❌ Error displaying pivoted sales data using crosstab: {e}")
    finally:
        if cur:
            cur.close()
            
# Task 9: Query Data using Joins

def query_data_joins(conn):
    """
    Executes and displays results for different types of JOIN queries.
    """
    try:
        cur = conn.cursor()

        # 1. INNER JOIN: Customers who have placed orders
        cur.execute("""
            SELECT
                c.customer_id,
                c.name AS customer_name,
                o.order_id,
                o.order_date,
                o.total_amount
            FROM customers c
            INNER JOIN orders o ON c.customer_id = o.customer_id
            ORDER BY c.customer_id;
        """)
        inner_join_results = cur.fetchall()
        print("\n✅ INNER JOIN: Customers with Orders")
        print("------------------------------------")
        if inner_join_results:
            for row in inner_join_results:
                print(f"Customer ID: {row[0]}, Name: {row[1]}, Order ID: {row[2]}, Order Date: {row[3]}, Total Amount: {row[4]}")
        else:
            print("No customers found with orders.")

        # 2. LEFT JOIN: All customers and their orders (if any)
        cur.execute("""
            SELECT
                c.customer_id,
                c.name AS customer_name,
                o.order_id,
                o.order_date,
                o.total_amount
            FROM customers c
            LEFT JOIN orders o ON c.customer_id = o.customer_id
            ORDER BY c.customer_id;
        """)
        left_join_results = cur.fetchall()
        print("\n✅ LEFT JOIN: All Customers and Orders (if any)")
        print("-----------------------------------------------")
        if left_join_results:
            for row in left_join_results:
                order_id_str = str(row[2]) if row[2] else 'No Order' # Handle NULL Order ID
                order_date_str = str(row[3]) if row[3] else 'N/A' # Handle NULL Order Date
                total_amount_str = str(row[4]) if row[4] else '0.00' # Handle NULL Total Amount
                print(f"Customer ID: {row[0]}, Name: {row[1]}, Order ID: {order_id_str}, Order Date: {order_date_str}, Total Amount: {total_amount_str}")
        else:
            print("No customer data found.")

        # 3. RIGHT JOIN: All orders and their customers (if any - should behave like INNER JOIN here due to data)
        cur.execute("""
            SELECT
                c.customer_id,
                c.name AS customer_name,
                o.order_id,
                o.order_date,
                o.total_amount
            FROM customers c
            RIGHT JOIN orders o ON c.customer_id = o.customer_id
            ORDER BY o.order_id;
        """)
        right_join_results = cur.fetchall()
        print("\n✅ RIGHT JOIN: All Orders and Customers (if any)")
        print("------------------------------------------------")
        if right_join_results:
            for row in right_join_results:
                customer_id_str = str(row[0]) if row[0] else 'No Customer' # Handle potential NULL Customer ID (though unlikely with current data)
                customer_name_str = row[1] if row[1] else 'N/A' # Handle potential NULL customer name
                print(f"Customer ID: {customer_id_str}, Name: {customer_name_str}, Order ID: {row[2]}, Order Date: {row[3]}, Total Amount: {row[4]}")
        else:
            print("No order data found.")


        # 4. FULL JOIN: All customers and orders, including unmatched records from both
        cur.execute("""
            SELECT
                c.customer_id,
                c.name AS customer_name,
                o.order_id,
                o.order_date,
                o.total_amount
            FROM customers c
            FULL JOIN orders o ON c.customer_id = o.customer_id
            ORDER BY c.customer_id, o.order_id;
        """)
        full_join_results = cur.fetchall()
        print("\n✅ FULL JOIN: All Customers and Orders (including unmatched)")
        print("--------------------------------------------------------")
        if full_join_results:
            for row in full_join_results:
                customer_id_str = str(row[0]) if row[0] else 'No Customer' # Handle NULL Customer ID
                customer_name_str = row[1] if row[1] else 'N/A' # Handle NULL customer name
                order_id_str = str(row[2]) if row[2] else 'No Order' # Handle NULL Order ID
                order_date_str = str(row[3]) if row[3] else 'N/A' # Handle NULL Order Date
                total_amount_str = str(row[4]) if row[4] else '0.00' # Handle NULL Total Amount

                print(f"Customer ID: {customer_id_str}, Name: {customer_name_str}, Order ID: {order_id_str}, Order Date: {order_date_str}, Total Amount: {total_amount_str}")
        else:
            print("No data found for customers or orders.")

        # 5. SELF JOIN: Find employees who report to the same manager
        cur.execute("""
            SELECT
                e1.employee_id AS emp1_id,
                e1.name AS emp1_name,
                e2.employee_id AS emp2_id,
                e2.name AS emp2_name,
                e1.manager_id
            FROM employees e1
            INNER JOIN employees e2 ON e1.manager_id = e2.manager_id
            WHERE e1.employee_id != e2.employee_id
            ORDER BY e1.manager_id, e1.employee_id;
        """)
        self_join_results = cur.fetchall()
        print("\n✅ SELF JOIN: Employees under Same Manager")
        print("------------------------------------------")
        if self_join_results:
            for row in self_join_results:
                print(f"Employee 1 ID: {row[0]}, Employee 1 Name: {row[1]}, Employee 2 ID: {row[2]}, Employee 2 Name: {row[3]}, Manager ID: {row[4]}")
        else:
            print("No employees found under the same manager.")


        cur.close()

    except Exception as e:
        print(f"❌ Error querying data with joins: {e}")
    finally:
        if cur:
            cur.close()
            
 # Task 10: UNION and UNION ALL

def task_union_union_all(conn):
    """
    Executes and displays results for UNION and UNION ALL operations based on
    the business requirements for Task 10.
    """
    try:
        cur = conn.cursor()

        # 1. UNION: Retrieve all customers and employees in a single list (distinct names)
        cur.execute("""
            SELECT name, 'Customer' AS type FROM customers
            UNION
            SELECT name, 'Employee' AS type FROM employees;
        """)
        union_results = cur.fetchall()

        print("\n✅ UNION: Combined List of Customers and Employees (Distinct Names)")
        print("---------------------------------------------------------------")
        if union_results:
            for row in union_results:
                print(f"Name: {row[0]}, Type: {row[1]}")
        else:
            print("No data found to UNION Customers and Employees.")

        # 2. UNION ALL: Retrieve all active and inactive orders in a single result set (all orders)
        # Assuming all orders in the 'orders' table are considered 'active' for this example.
        # To demonstrate UNION ALL, we'll select from 'orders' twice and label them as 'Active' and 'Inactive'
        # In a real system, 'inactive' orders might be in a separate table or have a status flag.

        cur.execute("""
            SELECT order_id, order_date, 'Active' AS order_status FROM orders
            UNION ALL
            SELECT order_id, order_date, 'Inactive' AS order_status FROM orders
            WHERE order_date < CURRENT_DATE - INTERVAL '1 year'; -- Hypothetically consider orders older than 1 year as inactive
        """)
        union_all_results = cur.fetchall()

        print("\n✅ UNION ALL: Combined List of Active and Inactive Orders (All Orders)")
        print("------------------------------------------------------------------")
        if union_all_results:
            for row in union_all_results:
                print(f"Order ID: {row[0]}, Order Date: {row[1]}, Status: {row[2]}")
        else:
            print("No order data found to UNION ALL Active and Inactive Orders.")

        cur.close()

    except Exception as e:
        print(f"❌ Error performing UNION/UNION ALL operations for Task 10: {e}")
    finally:
        if cur:
            cur.close()
            
#================================================= task 11 update data =================================================
def demonstrate_data_updates(conn):
    """
    Demonstrates data update operations: price increase, salary update, stock update.
    """
    try:
        cur = conn.cursor()

        # --- 1. Increase the price of all products in the Electronics category by 10%. ---
        print("\n--- 1. Increase Electronics Product Prices by 10% ---")

        # Before update: Retrieve current prices for Electronics category
        cur.execute("SELECT product_id, name, price FROM products WHERE category = 'Electronics';")
        electronics_products_before = cur.fetchall()
        print("\nElectronics products prices before update:")
        for row in electronics_products_before:
            print(f"Product ID: {row[0]}, Name: {row[1]}, Price: {row[2]}")

        # Perform the price update
        cur.execute("""
            UPDATE products
            SET price = price * 1.10
            WHERE category = 'Electronics';
        """)
        conn.commit()
        print("✅ Electronics product prices increased by 10%.")

        # After update: Retrieve updated prices for Electronics category
        cur.execute("SELECT product_id, name, price FROM products WHERE category = 'Electronics';")
        electronics_products_after = cur.fetchall()
        print("\nElectronics products prices after update:")
        for row in electronics_products_after:
            print(f"Product ID: {row[0]}, Name: {row[1]}, Price: {row[2]}")


        # --- 2. Update the salary of employees working for more than 5 years by 5%. ---
        print("\n--- 2. Update Employee Salaries (5% increase for > 5 years service) ---")

        # Add hire_date column if it doesn't exist
        cur.execute("ALTER TABLE Employees ADD COLUMN IF NOT EXISTS hire_date DATE;")
        conn.commit()

        # Update hire_date for employees if it's currently NULL (example dates - adjust as needed)
        cur.execute("UPDATE Employees SET hire_date = '2016-01-15' WHERE employee_id = 1 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2021-03-20' WHERE employee_id = 2 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2022-05-10' WHERE employee_id = 3 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2019-11-01' WHERE employee_id = 4 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2023-01-05' WHERE employee_id = 5 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2018-09-22' WHERE employee_id = 6 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2022-12-12' WHERE employee_id = 7 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2017-07-08' WHERE employee_id = 8 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2021-08-18' WHERE employee_id = 9 AND hire_date IS NULL;")
        cur.execute("UPDATE Employees SET hire_date = '2020-05-03' WHERE employee_id = 10 AND hire_date IS NULL;")
        conn.commit()


        # Before update: Retrieve salaries of eligible employees
        cur.execute("""
            SELECT employee_id, name, salary, hire_date
            FROM Employees
            WHERE hire_date < CURRENT_DATE - INTERVAL '5 years';
        """)
        eligible_employees_before = cur.fetchall()
        print("\nEligible employees for salary increase before update:")
        for row in eligible_employees_before:
            print(f"Employee ID: {row[0]}, Name: {row[1]}, Salary: {row[2]}, Hire Date: {row[3]}")

        # Perform the salary update
        cur.execute("""
            UPDATE Employees
            SET salary = salary * 1.05
            WHERE hire_date < CURRENT_DATE - INTERVAL '5 years';
        """)
        conn.commit()
        print("✅ Salaries updated for employees working more than 5 years.")

        # After update: Retrieve salaries of updated employees
        cur.execute("""
            SELECT employee_id, name, salary, hire_date
            FROM Employees
            WHERE hire_date < CURRENT_DATE - INTERVAL '5 years';
        """)
        eligible_employees_after = cur.fetchall()
        print("\nEligible employees for salary increase after update:")
        for row in eligible_employees_after:
            print(f"Employee ID: {row[0]}, Name: {row[1]}, Salary: {row[2]}, Hire Date: {row[3]}")


        # --- 3. Update product stock based on new supplier shipments. ---
        print("\n--- 3. Update Product Stock based on Shipments ---")

        # Create Shipments table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Shipments (
                shipment_id INT PRIMARY KEY,
                product_id INT REFERENCES Products(product_id),
                quantity INT NOT NULL,
                shipment_date DATE DEFAULT CURRENT_DATE
            );
        """)
        conn.commit()

        # Insert sample shipments (clearing existing data for demonstration)
        cur.execute("DELETE FROM Shipments;") # Clear existing shipment data
        cur.executemany("""
            INSERT INTO Shipments (shipment_id, product_id, quantity) VALUES (%s, %s, %s);
        """, [
            (101, 1, 50),  # 50 more Cotton Kurtas
            (102, 10, 20), # 20 more Mixer Grinders
            (103, 21, 10), # 10 more Smart LED TVs
            (104, 30, 100),# 100 more Wooden Toys
            (105, 40, 25)   # 25 more Basmati Rice
        ])
        conn.commit()
        print("✅ Sample shipments data inserted.")


        # Before update: Retrieve stock levels of affected products
        product_ids_to_check = [1, 10, 21, 30, 40]
        placeholders = ','.join(['%s'] * len(product_ids_to_check)) # Create placeholders for IN clause
        cur.execute(f"""
            SELECT product_id, name, stock FROM products WHERE product_id IN ({placeholders});
        """, product_ids_to_check) # Pass product_ids as parameters
        products_stock_before = cur.fetchall()
        print("\nProduct stock levels before shipment update:")
        for row in products_stock_before:
            print(f"Product ID: {row[0]}, Name: {row[1]}, Stock: {row[2]}")


        # Perform the stock update using JOIN
        cur.execute("""
            UPDATE Products
            SET stock = Products.stock + Shipments.quantity
            FROM Shipments
            WHERE Products.product_id = Shipments.product_id;
        """)
        conn.commit()
        print("✅ Product stock levels updated based on shipments.")

        # After update: Retrieve updated stock levels of affected products
        cur.execute(f"""
            SELECT product_id, name, stock FROM products WHERE product_id IN ({placeholders});
        """, product_ids_to_check) # Re-use the placeholders and product_ids
        products_stock_after = cur.fetchall()
        print("\nProduct stock levels after shipment update:")
        for row in products_stock_after:
            print(f"Product ID: {row[0]}, Name: {row[1]}, Stock: {row[2]}")


        cur.close()

    except Exception as e:
        print(f"❌ Error demonstrating data update operations: {e}")
    finally:
        if cur:
            cur.close()
            
#================================================= task 12 delete data =================================================
            
def demonstrate_data_deletion(conn):
    """
    Demonstrates data deletion operations:
    1. Delete inactive customers (no orders in last 2 years).
    2. Delete an order and ensure order items are also deleted (Cascading Delete).
    3. Truncate the employee audit table to reset logs.
    """
    try:
        cur = conn.cursor()

        # --- 1. Delete inactive customers (who haven’t ordered in the last 2 years). ---
        print("\n--- 1. Delete Inactive Customers (No orders in last 2 years) ---")

        # Before deletion: Count of inactive customers
        cur.execute("""
            SELECT COUNT(*)
            FROM customers
            WHERE customer_id NOT IN (SELECT DISTINCT customer_id FROM orders WHERE order_date >= CURRENT_DATE - INTERVAL '2 years');
        """)
        inactive_customers_count_before = cur.fetchone()[0]
        print(f"Inactive customers count before deletion: {inactive_customers_count_before}")

        # Perform the deletion of inactive customers
        cur.execute("""
            DELETE FROM customers
            WHERE customer_id NOT IN (SELECT DISTINCT customer_id FROM orders WHERE order_date >= CURRENT_DATE - INTERVAL '2 years');
        """)
        deleted_customer_count = cur.rowcount
        conn.commit()
        print(f"✅ {deleted_customer_count} inactive customers deleted.")

        # After deletion: Count of inactive customers
        cur.execute("""
            SELECT COUNT(*)
            FROM customers
            WHERE customer_id NOT IN (SELECT DISTINCT customer_id FROM orders WHERE order_date >= CURRENT_DATE - INTERVAL '2 years');
        """)
        inactive_customers_count_after = cur.fetchone()[0]
        print(f"Inactive customers count after deletion: {inactive_customers_count_after}")


        # --- 2. Delete an order and ensure order items are also deleted (Cascading Delete). ---
        print("\n--- 2. Delete Order (and Cascade Delete Order Items) ---")

        order_id_to_delete = 1  # Choose an order ID to delete (e.g., Order ID 1)

        # Before deletion: Count of order items for the order to be deleted
        cur.execute("SELECT COUNT(*) FROM order_items WHERE order_id = %s;", (order_id_to_delete,))
        order_items_count_before = cur.fetchone()[0]
        print(f"Order items count for Order ID {order_id_to_delete} before deletion: {order_items_count_before}")

        # Perform order deletion
        cur.execute("DELETE FROM orders WHERE order_id = %s;", (order_id_to_delete,))
        deleted_order_count = cur.rowcount # Should be 1 if order existed
        conn.commit()
        print(f"✅ Order ID {order_id_to_delete} deleted.")

        # After deletion: Count of order items for the deleted order (should be 0 due to CASCADE DELETE)
        cur.execute("SELECT COUNT(*) FROM order_items WHERE order_id = %s;", (order_id_to_delete,))
        order_items_count_after = cur.fetchone()[0]
        print(f"Order items count for Order ID {order_id_to_delete} after deletion: {order_items_count_after} (Cascade Delete verified)")


        # --- 3. Truncate the audit table to reset logs. ---
        print("\n--- 3. Truncate Employee Audit Table ---")

        # Before truncation: Count of records in audit table
        cur.execute("SELECT COUNT(*) FROM employee_audit;")
        audit_table_count_before = cur.fetchone()[0]
        print(f"Records in employee_audit table before truncation: {audit_table_count_before}")

        # Perform truncation
        cur.execute("TRUNCATE TABLE employee_audit;")
        conn.commit()
        print("✅ employee_audit table truncated (logs reset).")

        # After truncation: Count of records in audit table (should be 0)
        cur.execute("SELECT COUNT(*) FROM employee_audit;")
        audit_table_count_after = cur.fetchone()[0]
        print(f"Records in employee_audit table after truncation: {audit_table_count_after}")


        cur.close()

    except Exception as e:
        print(f"❌ Error demonstrating data deletion operations: {e}")
    finally:
        if cur:
            cur.close()

#================================================= task 13 stored procedures =================================================   
            
def demonstrate_stored_procedures(conn):
    """
    Demonstrates stored procedure operations: CRUD customer, get orders, update stock, sales report.
    """
    try:
        cur = conn.cursor()

        # --- 1. sp_AddCustomer: Add a new customer ---
        print("\n--- 1. Demonstrate sp_AddCustomer ---")
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_AddCustomer(
                p_name TEXT,
                p_email TEXT,
                p_phone TEXT,
                p_address TEXT
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                INSERT INTO customers (name, email, phone, address)
                VALUES (p_name, p_email, p_phone, p_address);
                COMMIT;
            END;
            $$;
        """)
        conn.commit()
        print("✅ Stored procedure sp_AddCustomer created.")

        new_customer_data = ("Priya Sharma", "priya.sharma@example.com", "9876543210", "456 Oak Avenue")
        cur.callproc('sp_AddCustomer', new_customer_data)
        print(f"✅ Customer '{new_customer_data[0]}' added using sp_AddCustomer.")

        cur.execute("SELECT * FROM customers WHERE name = %s;", (new_customer_data[0],))
        added_customer = cur.fetchone()
        print("\nNew customer details:")
        print(f"Customer ID: {added_customer[0]}, Name: {added_customer[1]}, Email: {added_customer[2]}, Phone: {added_customer[3]}, Address: {added_customer[4]}")


        # --- 2. sp_UpdateCustomer: Update customer details ---
        print("\n--- 2. Demonstrate sp_UpdateCustomer ---")
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_UpdateCustomer(
                p_customer_id INT,
                p_name TEXT,
                p_email TEXT,
                p_phone TEXT,
                p_address TEXT
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                UPDATE customers
                SET name = p_name,
                    email = p_email,
                    phone = p_phone,
                    address = p_address
                WHERE customer_id = p_customer_id;
                COMMIT;
            END;
            $$;
        """)
        conn.commit()
        print("✅ Stored procedure sp_UpdateCustomer created.")

        customer_id_to_update = added_customer[0]
        updated_customer_data = ("Priya S. Verma", "priya.verma@example.com", "9876543210", "456 New Oak Avenue")
        cur.callproc('sp_UpdateCustomer', (customer_id_to_update,) + updated_customer_data)
        print(f"✅ Customer ID {customer_id_to_update} updated using sp_UpdateCustomer.")

        cur.execute("SELECT * FROM customers WHERE customer_id = %s;", (customer_id_to_update,))
        updated_customer = cur.fetchone()
        print("\nUpdated customer details:")
        print(f"Customer ID: {updated_customer[0]}, Name: {updated_customer[1]}, Email: {updated_customer[2]}, Phone: {updated_customer[3]}, Address: {updated_customer[4]}")


        # --- 3. sp_DeleteCustomer: Delete customer (if no active orders) ---
        print("\n--- 3. Demonstrate sp_DeleteCustomer ---")
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_DeleteCustomer(p_customer_id INT)
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM orders WHERE customer_id = p_customer_id AND order_status != 'completed') THEN
                    DELETE FROM customers WHERE customer_id = p_customer_id;
                    COMMIT;
                    RAISE NOTICE 'Customer with ID % deleted successfully.', p_customer_id;
                ELSE
                    RAISE EXCEPTION 'Customer with ID % has active orders and cannot be deleted.', p_customer_id;
                END IF;
            END;
            $$;
        """)
        conn.commit()
        print("✅ Stored procedure sp_DeleteCustomer created.")

        customer_id_to_delete = updated_customer[0]

        # Try to delete - should succeed as we assume no active orders for this newly added customer
        try:
            cur.callproc('sp_DeleteCustomer', (customer_id_to_delete,))
            print(f"✅ Successfully called sp_DeleteCustomer for Customer ID {customer_id_to_delete}.")
        except Exception as e:
            print(f"❌ Error calling sp_DeleteCustomer for Customer ID {customer_id_to_delete}: {e}") # Exception expected if customer had active orders


        cur.execute("SELECT * FROM customers WHERE customer_id = %s;", (customer_id_to_delete,))
        deleted_customer_check = cur.fetchone()
        print(f"\nCustomer ID {customer_id_to_delete} exists after (attempted) deletion: {'No' if deleted_customer_check is None else 'Yes'}")


        # --- 4. sp_GetCustomerOrders: Retrieve orders by customer ---
        print("\n--- 4. Demonstrate sp_GetCustomerOrders ---")
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_GetCustomerOrders(p_customer_id INT)
            LANGUAGE plpgsql
            AS $$
            BEGIN
                SELECT * FROM orders WHERE customer_id = p_customer_id;
            END;
            $$;
        """)
        conn.commit()
        print("✅ Stored procedure sp_GetCustomerOrders created.")

        customer_id_for_orders = 1 # Example customer with orders
        cur.callproc('sp_GetCustomerOrders', (customer_id_for_orders,))
        customer_orders = cur.fetchall()

        print(f"\nOrders for Customer ID {customer_id_for_orders}:")
        if customer_orders:
            for order in customer_orders:
                print(f"Order ID: {order[0]}, Order Date: {order[2]}, Total Amount: {order[4]}, Status: {order[5]}")
        else:
            print("No orders found for this customer.")


        # --- 5. sp_AddProductStock: Increase product stock ---
        print("\n--- 5. Demonstrate sp_AddProductStock ---")
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_AddProductStock(
                p_product_id INT,
                p_quantity INT
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                UPDATE products
                SET stock = stock + p_quantity
                WHERE product_id = p_product_id;
                COMMIT;
            END;
            $$;
        """)
        conn.commit()
        print("✅ Stored procedure sp_AddProductStock created.")

        product_id_stock_update = 1 # Example product
        stock_to_add = 30

        cur.execute("SELECT stock FROM products WHERE product_id = %s;", (product_id_stock_update,))
        stock_before = cur.fetchone()[0]
        print(f"\nStock for Product ID {product_id_stock_update} before update: {stock_before}")

        cur.callproc('sp_AddProductStock', (product_id_stock_update, stock_to_add))
        print(f"✅ Added {stock_to_add} stock for Product ID {product_id_stock_update} using sp_AddProductStock.")

        cur.execute("SELECT stock FROM products WHERE product_id = %s;", (product_id_stock_update,))
        stock_after = cur.fetchone()[0]
        print(f"Stock for Product ID {product_id_stock_update} after update: {stock_after}")


        # --- 6. sp_GenerateSalesReport: Monthly sales report per store ---
        print("\n--- 6. Demonstrate sp_GenerateSalesReport ---")
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_GenerateSalesReport(p_month TEXT, p_year INT)
            LANGUAGE plpgsql
            AS $$
            BEGIN
                SELECT
                    s.name AS store_name,
                    COALESCE(SUM(oi.quantity * p.price), 0) AS monthly_sales
                FROM stores s
                LEFT JOIN orders o ON s.store_id = o.store_id
                LEFT JOIN order_items oi ON o.order_id = oi.order_id
                LEFT JOIN products p ON oi.product_id = p.product_id
                WHERE EXTRACT(MONTH FROM o.order_date) = EXTRACT(MONTH FROM TO_DATE(p_month, 'Mon'))
                  AND EXTRACT(YEAR FROM o.order_date) = p_year
                GROUP BY s.name
                ORDER BY s.name;
            END;
            $$;
        """)
        conn.commit()
        print("✅ Stored procedure sp_GenerateSalesReport created.")

        report_month = 'Jan'
        report_year = 2024
        cur.callproc('sp_GenerateSalesReport', (report_month, report_year))
        sales_report = cur.fetchall()

        print(f"\nSales Report for {report_month} {report_year}:")
        if sales_report:
            for row in sales_report:
                print(f"Store: {row[0]}, Sales: {row[1]:.2f}")
        else:
            print("No sales data found for this month and year.")


        cur.close()
        print("\n✅ Task 13: Stored procedures demonstrated successfully!")

    except Exception as e:
        print(f"❌ Error demonstrating stored procedures (Task 13): {e}")
    finally:
        if cur:
            cur.close()
       
#================================================= task 14 create tables =================================================            
def create_stored_procedures(conn):
    """
    Creates stored procedures for CRUD operations on the database.
    """
    try:
        cur = conn.cursor()

        # 1. sp_AddCustomer: Add a new customer
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_AddCustomer (
                p_name VARCHAR(255),
                p_email VARCHAR(255),
                p_phone VARCHAR(20),
                p_city VARCHAR(255)
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                INSERT INTO customers (customer_id, name, email, phone, city)
                VALUES (nextval('customers_customer_id_seq'), p_name, p_email, p_phone, p_city); -- Assuming you have a sequence for customer_id
                COMMIT;
            END;
            $$;
        """)

        # 2. sp_UpdateCustomer: Update customer details
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_UpdateCustomer (
                p_customer_id INT,
                p_name VARCHAR(255),
                p_email VARCHAR(255),
                p_phone VARCHAR(20),
                p_city VARCHAR(255)
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                UPDATE customers
                SET name = p_name,
                    email = p_email,
                    phone = p_phone,
                    city = p_city
                WHERE customer_id = p_customer_id;
                COMMIT;
            END;
            $$;
        """)

        # 3. sp_DeleteCustomer: Delete a customer if they have no orders
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_DeleteCustomer (
                p_customer_id INT
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM orders WHERE customer_id = p_customer_id) THEN
                    DELETE FROM customers WHERE customer_id = p_customer_id;
                    COMMIT;
                ELSE
                    RAISE EXCEPTION 'Customer cannot be deleted because they have existing orders.';
                END IF;
            END;
            $$;
        """)

        # 4. sp_GetCustomerOrders: Retrieve all orders by a customer
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_GetCustomerOrders (
                p_customer_id INT
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                SELECT *
                FROM orders
                WHERE customer_id = p_customer_id;
            END;
            $$;
        """)

        # 5. sp_AddProductStock: Increase product stock
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_AddProductStock (
                p_product_id INT,
                p_quantity INT
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                UPDATE products
                SET stock = stock + p_quantity
                WHERE product_id = p_product_id;
                COMMIT;
            END;
            $$;
        """)

        # 6. sp_GenerateSalesReport: Generate monthly sales report per store
        cur.execute("""
            CREATE OR REPLACE PROCEDURE sp_GenerateSalesReport (
                p_year INT,
                p_month INT
            )
            LANGUAGE plpgsql
            AS $$
            DECLARE
                report_cursor CURSOR FOR
                    SELECT
                        s.name AS store_name,
                        SUM(oi.price * oi.quantity) AS monthly_revenue
                    FROM stores s
                    JOIN orders o ON s.store_id = o.store_id
                    JOIN order_items oi ON o.order_id = oi.order_id
                    WHERE EXTRACT(YEAR FROM o.order_date) = p_year AND EXTRACT(MONTH FROM o.order_date) = p_month
                    GROUP BY s.name
                    ORDER BY monthly_revenue DESC;
                report_record RECORD;
            BEGIN
                OPEN report_cursor;
                LOOP
                    FETCH NEXT FROM report_cursor INTO report_record;
                    EXIT WHEN NOT FOUND;
                    -- You can process or display the report_record here, for example:
                    RAISE NOTICE 'Store: %, Revenue: %', report_record.store_name, report_record.monthly_revenue;
                END LOOP;
                CLOSE report_cursor;
            END;
            $$;
        """)


        conn.commit()
        cur.close()
        print("✅ Stored procedures created successfully!")

    except Exception as e:
        print(f"❌ Error creating stored procedures: {e}")
    finally:
        if cur:
            cur.close()

#================================================= task 14 data export =================================================            
            
def get_monthly_revenue_per_store(conn):
    """
    Retrieves monthly revenue per store from the store_revenue view.
    """
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM store_revenue;")
        results = cur.fetchall()
        cur.close()
        return results
    except Exception as e:
        print(f"❌ Error retrieving monthly revenue per store: {e}")
        return None

def get_customer_total_spending(conn):
    """
    Retrieves a list of customers and their total spending.
    """
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                c.customer_id,
                c.name AS customer_name,
                COALESCE(SUM(o.total_amount), 0) AS total_spending
            FROM customers c
            LEFT JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id, c.name
            ORDER BY total_spending DESC;
        """)
        results = cur.fetchall()
        cur.close()
        return results
    except Exception as e:
        print(f"❌ Error retrieving customer total spending: {e}")
        return None

def export_to_csv(data, filename, header):
    """
    Exports data (list of tuples) to a CSV file.
    """
    try:
        df = pd.DataFrame(data, columns=header)
        df.to_csv(filename, index=False)
        print(f"✅ Data exported to CSV file: {filename}")
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")

def export_to_xlsx(data, filename, sheet_name, header):
    """
    Exports data (list of tuples) to an XLSX file.
    """
    try:
        df = pd.DataFrame(data, columns=header)
        df.to_excel(filename, sheet_name=sheet_name, index=False)
        print(f"✅ Data exported to XLSX file: {filename} - Sheet: {sheet_name}")
    except Exception as e:
        print(f"❌ Error exporting to XLSX: {e}")

def export_monthly_revenue_to_file(conn, file_format):
    """
    Exports monthly revenue per store to either CSV or XLSX format based on user choice.
    """
    monthly_revenue_data = get_monthly_revenue_per_store(conn)
    if monthly_revenue_data:
        revenue_header = ["store_id", "store_name", "total_revenue"]
        if file_format.upper() == "CSV":
            export_to_csv(monthly_revenue_data, "monthly_revenue_per_store.csv", revenue_header)
        elif file_format.upper() == "XLSX":
            export_to_xlsx(monthly_revenue_data, "monthly_revenue_per_store.xlsx", "Store Revenue", revenue_header)
        else:
            print("❌ Invalid file format. Please choose CSV or XLSX.")
    else:
        print("No monthly revenue data to export.")


def export_customer_spending_to_file(conn, file_format):
    """
    Exports customer total spending data to either CSV or XLSX format based on user choice.
    """
    customer_spending_data = get_customer_total_spending(conn)
    if customer_spending_data:
        spending_header = ["customer_id", "customer_name", "total_spending"]
        if file_format.upper() == "CSV":
            export_to_csv(customer_spending_data, "customer_total_spending.csv", spending_header)
        elif file_format.upper() == "XLSX":
            export_to_xlsx(customer_spending_data, "customer_total_spending.xlsx", "Customer Spending", spending_header)
        else:
            print("❌ Invalid file format. Please choose CSV or XLSX.")
    else:
        print("No customer spending data to export.")


def task_14_export_data(conn):
    """
    Executes Task 14: Data Export - Exports monthly revenue and customer spending to CSV/XLSX.
    """
    print("\n🚀 Task 14: Data Export - Started...")

    # 1. Export monthly revenue per store
    monthly_revenue_data = get_monthly_revenue_per_store(conn)
    if monthly_revenue_data:
        revenue_header = ["store_id", "store_name", "total_revenue"]
        export_to_csv(monthly_revenue_data, "monthly_revenue_per_store.csv", revenue_header)
        export_to_xlsx(monthly_revenue_data, "monthly_revenue_per_store.xlsx", "Store Revenue", revenue_header)

    # 2. Export list of customers and their total spending
    customer_spending_data = get_customer_total_spending(conn)
    if customer_spending_data:
        spending_header = ["customer_id", "customer_name", "total_spending"]
        export_to_csv(customer_spending_data, "customer_total_spending.csv", spending_header)
        export_to_xlsx(customer_spending_data, "customer_total_spending.xlsx", "Customer Spending", spending_header)

    print("✅ Task 14: Data Export - Completed!\n")


#================================================= main =================================================

# Establish connection and create tables
conn = connect_db()
inp = int(input("""1. Create tables
2. Create indexes
3. Create views
4. Create triggers
5. Insert sample data
6. Load XLSX to DB
7. Display employee hierarchy
8. Display monthly sales pivot table
9. Display data joins
10. UNION and UNION ALL
11. Demonstrate data updates
12. Demonstrate data deletions
13. Demonstrate stored procedures
14. Export data to CSV/XLSX
Enter your choice (1/2/3/4/5/6/7/8/9/10/11/12/13/14) -
-> """))

if conn:
    if inp == 1:
        create_tables(conn)
    elif inp == 2:
        create_indexes(conn)
    elif inp == 3:
        create_views(conn)
    elif inp == 4:
        create_triggers(conn)
    elif inp == 5:
        insert_sample_data(conn)
    elif inp == 6:
            file_path = input("Enter XLSX file path: ")
            table_name = input("Enter table name: ")
            load_xlsx_to_db(file_path, table_name, conn)
    elif inp == 7:
        display_employee_hierarchy(conn)
    elif inp == 8:
        display_monthly_sales_pivot_crosstab(conn)  
    elif inp == 9:
        query_data_joins(conn)
    elif inp == 10:
        task_union_union_all(conn)
    elif inp == 11:
        demonstrate_data_updates(conn)
    elif inp == 12:
        demonstrate_data_deletion(conn)
    elif inp == 13:
        create_stored_procedures(conn)
    elif inp == 14:  # Modified Task 14 implementation
                    export_type = int(input("""1. Export monthly revenue per store
2. Export customer spending

-> """))
                    file_format = input("Enter export file format (CSV or XLSX): ").strip()
                    if export_type == 1:
                        export_monthly_revenue_to_file(conn, file_format)
                    elif export_type == 2:
                        export_customer_spending_to_file(conn, file_format)
                    else:
                        print("❌ Invalid export type selected.")


    else:
        print("Invalid input!")
    conn.close()    
    
