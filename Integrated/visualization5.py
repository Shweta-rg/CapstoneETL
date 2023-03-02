# 5 Find and plot the percentage of applications approved for self-employed applicants.

from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
import findspark
findspark.init()
findspark.find()

sp = SparkSession.builder.appName("Customer").getOrCreate()


def test_self_percent():

    query = "(SELECT self_employed, count(application_status), \
                    (round(count(application_status)/(SELECT COUNT(Application_ID)\
                    FROM cdw_sapp_loan_application \
                    WHERE application_status = 'Y')*100, 2) ) as Percent \
                    FROM cdw_sapp_loan_application \
                    WHERE Application_status = 'Y' \
                    GROUP BY self_employed, application_status) as b"

    sp_sql1 = sp.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("user", "root") \
        .option("password", "password") \
        .option("dbtable", query) \
        .load()

    pd_sql1 = sp_sql1.toPandas()

    # pd_sql1.reset_index(inplace=True)
    pd_sql1['Percent'] = pd_sql1['Percent'].astype('float')
    print(pd_sql1.dtypes)

    pd_sql1.plot(kind='pie', x='self_employed', y='Percent',
                 autopct='%1.1f%%', figsize=(8, 8))
    plt.title('Percentage of approved self employed applicants')
    plt.xlabel('Self-Employed')
    # plt.ylabel('Percentage of Applications')
    # plt.yticks([i for i in ])
    plt.show()


def test_male_status():

    query2 = "(SELECT application_status, COUNT(application_status) AS count_applications, \
            (round(count(application_status)/(SELECT COUNT(*) FROM cdw_sapp_loan_application \
            WHERE Gender = 'Male' AND Married = 'Yes' )*100,2)) AS Percent \
            FROM cdw_sapp_loan_application \
            WHERE Gender = 'Male' AND Married = 'Yes' \
            GROUP BY application_status) as a"

    sp_sql2 = sp.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("user", "root") \
        .option("password", "password") \
        .option("dbtable", query2) \
        .load()
    pd_sql2 = sp_sql2.toPandas()

# pd_sql1.reset_index(inplace=True)
    pd_sql2['Percent'] = pd_sql2['Percent'].astype('float')
    print(pd_sql2.dtypes)

    pd_sql2.plot(kind='bar', x='application_status',
                 y='Percent', figsize=(8, 5))
    plt.title('The percentage of Applicants rejected/approved')
    plt.xlabel('Application Status')
    plt.ylabel('Percentage of Married Male Applicants')
    plt.show()


def test_top3_tran_mon():
    # Find and plot the top three months with the largest transaction data.

    query3 = "(SELECT month(timeid),MONTHNAME(timeid) AS Month, round(sum(TRANSACTION_value),2) AS Transaction_value \
                FROM cdw_sapp_credit_card \
                GROUP BY substr(timeid,1,6) \
                ORDER BY sum(TRANSACTION_value) DESC \
                LIMIT 3) as a"

    sp_sql3 = sp.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("user", "root") \
        .option("password", "password") \
        .option("dbtable", query3) \
        .load()

    pd_sql3 = sp_sql3.toPandas()
    pd_sql3.sort_values(by='month(timeid)', inplace=True)

    # pd_sql1.reset_index(inplace=True)
    pd_sql3['Transaction_value'] = pd_sql3['Transaction_value'].astype('float')
    print(pd_sql3.dtypes)

    pd_sql3.plot(kind='line', x='Month', y='Transaction_value', figsize=(8, 5))
    plt.title('Transaction value based on Month')
    plt.xlabel('MONTHs')
    plt.ylabel('Total Transaction Value')
    plt.show()


def test_healthcare():
    # Find and plot which branch processed the highest total dollar value of healthcare transactions.

    query4 = "(SELECT branch_code, round(SUM(transaction_value),2) AS Total_Transaction_Value  \
                FROM cdw_sapp_credit_card \
                WHERE transaction_type = 'Healthcare'\
                group by branch_code \
                ORDER BY SUM(transaction_value) DESC \
                LIMIT 5) as a"

    sp_sql4 = sp.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("user", "root") \
        .option("password", "password") \
        .option("dbtable", query4) \
        .load()

    pd_sql4 = sp_sql4.toPandas()
    # pd_sql4.sort_values(by='month(timeid)', inplace=True)

    # pd_sql1.reset_index(inplace=True)
    pd_sql4['Transaction_value'] = pd_sql4['Total_Transaction_Value'].astype(
        'float')
    print(pd_sql4.dtypes)

    pd_sql4.plot(kind='bar', x='branch_code',
                 y='Total_Transaction_Value', figsize=(8, 5))
    plt.title('Highest total dollar for healthcare transactions by Branch code')
    plt.ylabel('Total Transaction Value')
    plt.xlabel('Branch Code')
    plt.show()
