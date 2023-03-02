# Main Menu File

import pyinputplus as pyip
import pyspark as py
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyinputplus as pyip
import plotly.express as px
import plotly.graph_objects as go


from secret import username
from secret import password

import module21_1
import module22_3
import mod22_1_2
import funct2_last
import mod21_2
import mod21_3
import mod222new
import visualization
import visualization5


# Creating Spark Session
sp = SparkSession.builder.appName("Customer").getOrCreate()

df_sp_cust = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CUSTOMER") \
    .option("user", username) \
    .option("password", password) \
    .load()

df_sp_cc = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CREDIT_CARD") \
    .option("user", username) \
    .option("password", password) \
    .load()
df_sp_br = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone")  \
    .option("dbtable", "cdw_sapp_branch") \
    .option("user", username) \
    .option("password", password) \
    .load()
query = "(SELECT cc.*, cust.cust_zip \
        FROM cdw_sapp_credit_card as cc \
        JOIN cdw_sapp_customer as cust ON cc.CUST_SSN = cust.SSN) as a"
df_sp_cc_cust = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", query) \
    .option("user", username) \
    .option("password", password) \
    .load()
df_sp_cc_cust = df_sp_cc_cust.withColumn('Date',
                                         concat(df_sp_cc_cust['TIMEID'].
                                                substr(0, 4), lit('-'), df_sp_cc_cust['TIMEID'].
                                                substr(5, 2), lit('-'), df_sp_cc_cust['TIMEID'].substr(7, 2)))


df_pd_cust = df_sp_cust.toPandas()
list_ssn = list(df_pd_cust['SSN'])

pd_credit = df_sp_cc.toPandas()
list_cc = list(pd_credit['CUST_CC_NO'])
list_type = list(pd_credit['TRANSACTION_TYPE'].drop_duplicates())

pd_branch = df_sp_br.toPandas()
list_state = list(pd_branch['BRANCH_STATE'].drop_duplicates())
# list_state

query2 = "(SELECT br.branch_code , br.branch_state,cc.transaction_value \
            FROM cdw_sapp_credit_card as cc \
            JOIN cdw_sapp_branch as br ON cc.BRANCH_CODE = br.BRANCH_CODE) as b"

df_sp_cc_br = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", query2) \
    .option("user", "root") \
    .option("password", "password") \
    .load()

list_main_menu = ["Transactions made by customers by Zipcode",
                  "Count and total values of transactions for type",
                  "Total number & values of trans in a give state",
                  "Check the existing account details of a customer",
                  "Modify the existing account details of a customer",
                  "Monthly bill for credit card num for given month year",
                  "Display transactions by customer bet two dates",

                  "Data Analysis And visualization",
                  "Exit"]


while True:
    var_main_menu = pyip.inputMenu(list_main_menu, numbered=True)
    print(var_main_menu)
    if var_main_menu == 'Exit':
        break
    elif var_main_menu == 'Transactions made by customers by Zipcode':
        module21_1.test_call(df_sp_cc_cust)
    elif var_main_menu == 'Count and total values of transactions for type':
        mod21_2.test_tran_type_value(df_sp_cc, list_type)
    elif var_main_menu == "Total number & values of trans in a give state":
        mod21_3.test_tran_state(df_sp_cc_br, list_state)
    elif var_main_menu == "Check the existing account details of a customer":
        mod22_1_2.test_exist_cust(list_ssn, df_sp_cust)

    elif var_main_menu == "Modify the existing account details of a customer":
        mod222new.update_cust_details(df_sp_cust, list_ssn)

    elif var_main_menu == 'Display transactions by customer bet two dates':
        funct2_last.test_call2(df_sp_cc_cust, list_ssn)

    elif var_main_menu == 'Monthly bill for credit card num for given month year':
        module22_3.test_call3(df_sp_cc, list_cc)

    elif var_main_menu == "Data Analysis And visualization":

        while True:
            list_DAV_menu = [
                "DAV: TransType has a high rate of trans",
                "DAV: State has a high number of customers",
                "DAV: Customer with highest transaction amount",
                "DAV:Percent of applicatn approved  self-employ",
                "DAV:Percent of married male applicants",
                "top 3 months with the largest transaction data",
                "Branch with highest healthcare transactions",
                "Go Back To main"]
            var_DAV_menu = pyip.inputMenu(list_DAV_menu, numbered=True)
            print(var_DAV_menu)
            if var_DAV_menu == 'Go Back To main':
                break
            elif var_DAV_menu == 'DAV: TransType has a high rate of trans':
                visualization.transact_type_cnt(pd_credit)

            elif var_DAV_menu == "DAV: State has a high number of customers":
                visualization.state_transaction(df_pd_cust)

            elif var_DAV_menu == "DAV: Customer with highest transaction amount":
                visualization.cust_transact(pd_credit)
            elif var_DAV_menu == "DAV:Percent of applicatn approved  self-employ":
                visualization5.test_self_percent()
            elif var_DAV_menu == "DAV:Percent of married male applicants":
                visualization5.test_male_status()
            elif var_DAV_menu == "top 3 months with the largest transaction data":
                visualization5.test_top3_tran_mon()
            elif var_DAV_menu == "Branch with highest healthcare transactions":
                visualization5.test_healthcare()
