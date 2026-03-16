# 🍽️ Swiggy Food Delivery — End-to-End Data Engineering Project
### Microsoft Fabric | PySpark | Delta Lake | Data Warehouse | Semantic Model

## 📌 Project Overview
 
An end-to-end **Data Engineering pipeline** built on **Microsoft Fabric** that ingests raw Swiggy Food Delivery CSV data, applies multi-layer transformations using **PySpark Notebooks**, stores cleaned data as **Delta Tables** in a **Fabric Lakehouse**, loads it into a **Data Warehouse** for SQL analytics, and exposes a **Star Schema Semantic Model** (Fact + Dimension tables) for Power BI reporting.
 
## 🏗️ Architecture Diagram



**Data Flow:**
```
Source CSV File
    → Lakehouse CSV File Load
        → PySpark Notebook (Transformations)
            → Lakehouse Delta Table
                → Pipeline (Orchestration)
                    → Data Warehouse (SQL Analytics)
                        → Dataflow Gen2
                            → Semantic Model (Fact + Dim Tables)
```


