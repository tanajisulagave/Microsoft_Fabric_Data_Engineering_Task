#!/usr/bin/env python
# coding: utf-8

# ## NB_SwiggyFoodDelivery
# 
# null

# ##### **File reading from lahehouse in to Notebook**

# In[1]:


filepath="abfss://SwiggyFoodDelivery@onelake.dfs.fabric.microsoft.com/lh_SwiggyFoodDelivery.Lakehouse/Files"

df=spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load(filepath)


# In[2]:


display(df)


# In[25]:


from pyspark.sql.functions import col,when,trim,regexp_replace,regexp_extract,current_date,initcap,split,lit


# ##### **1.Handling Missing Values (Imputation) [Removing null and Blank values with Unkowon]**

# In[4]:


#.withColumn("delivery_time",when(col("delivery_time").isNull() | (trim(col("delivery_time"))==""),current_date()) ## todays Date
#.otherwise(col("delivery_time")))\

df_notnull = df\
.withColumn("online_order",when(col("online_order").isNull() | (trim(col("online_order"))==""),"Unknown")
.otherwise(col("online_order")))\
.withColumn("book_table",when(col("book_table").isNull() | (trim(col("book_table"))==""),"Unknown")
.otherwise(col("book_table")))\
.withColumn("distance_km",when(col("distance_km").isNull() | (trim(col("distance_km"))==""),0)
.otherwise(col("distance_km")))


# ##### **2. String Standardization & Typo Correction ("Bnegaluru" = "Bengaluru")**

# In[5]:


df_TypeCast = df_notnull.withColumn("city",\
    initcap(regexp_replace(
            trim(col("city")),"Bnegaluru","Bengaluru"
        )
    )
)


# In[6]:


display(df_TypeCast)


# ##### **3.Numerical Data Transformation**

# ###### **Clean Rating Column as "None**

# In[13]:


df_None = df_TypeCast.withColumn("rating_cleaned",
     when(col("rating").isNull() | (trim(col("rating")) == ""), None)
     .when(trim(col("rating")) == "NEW", None)   # treat NEW as null
     .otherwise(
         regexp_extract(trim(col("rating")), r"^(\d+\.?\d*)/5$", 1).cast("double")
     )
 )


# ###### **--Calculate Median for Rating**

# In[15]:


from pyspark.sql.functions import percentile_approx

Median_Rating = df_None.select(percentile_approx("rating_cleaned",0.5).alias("median_rating")\
).collect()[0]["median_rating"]

print(f"Median Rating Value : {Median_Rating}")


# ###### **--Rating Calculation**

# In[16]:


df_Rating_Transform = df_TypeCast.withColumn("rating_cleaned",when(col("rating").isNull(),f"{Median_Rating}/5")\
        .when(trim(col("rating"))=="NEW","0/5")\
        .otherwise(
            regexp_replace(trim(col("rating")),r"/s+","")
        )
)


# In[17]:


display(df_Rating_Transform)


# ##### **Split the value in rating column**

# In[18]:


df_Rating_Numeric = df_Rating_Transform.withColumn("rating_cleaned",
    split(
            regexp_replace(trim(col("rating_cleaned")),r"\s+",""),"/"
    ).getItem(0)
    .cast("float")
)


# ##### **old rating column drop and Column Name changed "rating-cleaned = rating**

# In[21]:


df_RenameColumn=df_Rating_Numeric.drop("rating").withColumnRenamed("rating_cleaned","rating")


# ##### **total_amount column need only positive values**

# In[22]:


df_Amount_positive=df_RenameColumn.filter(col("total_amount")>=0)


# ##### **4. Applying Conditional Logic**

# In[26]:


df_final=df_Amount_positive.withColumn("delivery_time",when(col("online_order")=="No",lit(None).cast("string"))\
                    .otherwise(col("delivery_time"))
                )\
                    .withColumn("distance_km",when(col("online_order")=="No",lit(None).cast("double"))\
                    .otherwise(col("distance_km"))
                )
                    


# In[27]:


display(df_final)


# ##### **Write Cleaned Data to Lakehouse as Delta Table**

# In[28]:


delta_table_name = "Swiggy_delta_table"

df_final.write.format("delta")\
                .mode("overwrite")\
                .option("overwritwSchema","true")\
                .saveAsTable(delta_table_name)

