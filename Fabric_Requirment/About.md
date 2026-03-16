# Bengaluru Food Delivery Analysis & Modeling 🍕📊

---

## **📌 Project Overview**
This project focuses on the end-to-end data engineering and analysis of a **Food Delivery Dataset** based in **Bengaluru, India**. The primary objective is to take a "messy" raw dataset of 4,000 records and perform advanced **Data Cleaning**, **Exploratory Data Analysis (EDA)** using SQL, and **Dimensional Modeling** (Star Schema).

---

## **🏗️ The Dataset**
The dataset simulates 4,000 food delivery orders with the following characteristics:
* **Geographic Focus:** Major Bengaluru hubs (Koramangala, Indiranagar, HSR Layout, etc.).
* **Complexity:** Includes intentional "dirty data" such as typos in city names, inconsistent rating formats (e.g., "4.5/5" vs "NEW"), and conditional null values.
* **Key Columns:** `order_id`, `order_time`, `restaurant`, `cuisine`, `total_amount`, `rating`, `location`, `online_order`, and `distance_km`.

---

## **🚀 Tasks to Complete**

### **1. Data Cleaning (PySpark)**
Transform the raw CSV into a "Gold" standard dataset by:
* **Standardizing Strings:** Fixing typos like "Bnegaluru" and trimming white spaces.
* **Type Casting:** Converting ratings from strings to floats and timestamps to proper date formats.
* **Integrity Logic:** Ensuring that offline orders do not have delivery times or distances.
* **Handling Nulls:** Imputing missing values for `online_order` and `book_table`.

### **2. Exploratory Data Analysis (SQL)**
Register the cleaned data as a Spark SQL view and answer business-critical questions:
* Identify top-performing restaurants and cuisines by revenue.
* Analyze delivery efficiency (Distance vs. Time) across different Bengaluru locations.
* Determine the impact of dining features (Table Booking) on average customer spend.

### **3. Dimensional Modeling (Data Warehousing)**
Refactor the flat CSV into a **Star Schema** to optimize for analytical reporting:
* **Fact Table:** `Fact_Orders` (contains keys and metrics like amount and quantity).
* **Dimension Tables:** `Dim_Restaurant`, `Dim_Location`, `Dim_Time`, and `Dim_Order_Type`.

---

## **📁 Data Dictionary**
| Column | Data Type | Description |
| :--- | :--- | :--- |
| **order_id** | String | Unique identifier for each order. |
| **order_time** | Timestamp | Time when the order was placed. |
| **delivery_time** | Timestamp | Time when the food was delivered (Online only). |
| **total_amount** | Float | Order value in INR (₹). |
| **rating** | Float | Cleaned numerical rating (0.0 - 5.0). |
| **location** | String | Specific area in Bengaluru. |
| **distance_km** | Float | Distance from restaurant to delivery point. |

---

## **📅 Submission Details**
* **Final Submission Date:** 20th March 2026
* **Deadline:** 11:59 PM IST
* **Format:** Ensure your GitHub repository includes your cleaning scripts, SQL queries, and the final cleaned dataset.

---

## **🛠️ Requirements**
* **PySpark**


---
