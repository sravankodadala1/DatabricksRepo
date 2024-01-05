# Databricks notebook source
# MAGIC %run "/Users/sravankumar04032001@gmail.com/Assignment2/Util.py"

# COMMAND ----------


url = 'https://reqres.in/api/users?page=2'
spark = SparkSession.builder.appName("SiteInfoProcessor").getOrCreate()
site_info_df = read_api_data(spark, url)

columns_to_drop = ["page", "per_page", "total", "total_pages", "support"]
site_info_df = drop_columns(site_info_df, columns_to_drop)

columns_to_explode = ["avatar", "email"]
site_info_df = explode_columns(site_info_df, columns_to_explode)

site_info_df = derive_site_address(site_info_df)
site_info_df = add_load_date(site_info_df)
site_info_df.show()

delta_path = "/site_info/persons"
write_to_delta(site_info_df, delta_path)

