# 3 .. --credit card
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
import findspark
findspark.init()
findspark.find()

sp = SparkSession.builder.appName("customer").getOrCreate()

df_sp_cc = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CREDIT_CARD") \
    .option("user", "root") \
    .option("password", "password") \
    .load()

df_sp_cc.show()
pd_credit = df_sp_cc.toPandas()


def transact_type_cnt(pd_credit):
    # 3.1 .. Find and plot which transaction type has a high rate of transactions.--credit card

    df1 = pd_credit[['TRANSACTION_TYPE', 'TRANSACTION_VALUE']
                    ].groupby(pd_credit['TRANSACTION_TYPE']).sum()
    df1.reset_index(inplace=True)
    # sns.relplot(
    #     data=df1,
    #     x="TRANSACTION_TYPE", y="TRANSACTION_VALUE", col="time",
    #     style="smoker", size="size",
    # )
    # sns.color_palette("viridis", as_cmap=True)

    df1.plot(kind='bar', x='TRANSACTION_TYPE',
             y='TRANSACTION_VALUE', color=(0.2, 0.4, 0.6, 0.6))

    plt.title('Transaction Counts by Type')
    plt.xlabel('Transaction Type')
    plt.ylabel('Transaction Count')
    # for index, value in enumerate(df1):
    #     label = format(int(value, ','))
    # plt.annotate(label, xy=(value-650, index-0.10), color='Black')
    plt.show()


# transact_type_cnt(pd_credit)

# 3.2:Find and plot which state has a high number of customers.


def state_transaction(df_pd_cust):
    # 3.2Find and plot which state has a high number of customers..customer
    df_cust1 = df_pd_cust[['CUST_STATE']].groupby(
        df_pd_cust['CUST_STATE']).count()
    df_cust1.rename(columns={'CUST_STATE': 'COUNT'}, inplace=True)

    df_cust1.reset_index(inplace=True)
    df_cust1
    fig = px.sunburst(df_cust1, path=['CUST_STATE', 'COUNT'], values='COUNT')
    fig.show()
    # df_cust1.plot(kind='bar', x='CUST_STATE', y='COUNT', figsize=(8, 8))
    # plt.title('Transaction Counts by State')
    # plt.xlabel('Transaction Type')

    # plt.ylabel('Transaction Count')
    plt.show()

# state_transaction(df_pd_cust)


# 3.3Find and plot the sum of all transactions for the top 10 customers, and which customer has the highest transaction amount.
# hint(use CUST_SSN).
# top 10 customers with higher transactions
def cust_transact(pd_credit):
    # sum of all transactions for the top 10 customers,with high transac amt-- creditcard

    pd_credit['CUST_SSN'] = pd_credit['CUST_SSN'].astype('string')

    df_cc1 = pd_credit[['CUST_SSN', 'TRANSACTION_VALUE']
                       ].groupby(pd_credit['CUST_SSN']).sum()
    df_cc1.reset_index(inplace=True)
    df_cc1 = df_cc1.sort_values(by=['TRANSACTION_VALUE'], ascending=False)
    df_cctop = df_cc1.head(10)

    fig = px.scatter(df_cctop, x="CUST_SSN", y="TRANSACTION_VALUE",
                     title='Top 10 customers', size='TRANSACTION_VALUE', hover_name='CUST_SSN')
    fig.show()

    # df_cctop.plot(kind='barh', x='CUST_SSN',
    #               y='TRANSACTION_VALUE', figsize=(20, 10))
    # plt.title('Top 10 customers')
    # plt.xlabel('Customers-ID')
    # # plt.xticks('CUST_SSN')
    # # plt.xticks([i for i in df_cctop['CUST_SSN']], 'CUST_SSN', rotation='65')
    # plt.ylabel('Transaction value')
    # plt.show()
    # for index, value in enumerate(df_cctop):
    #     label = format(int(value), ',')
    # plt.annotate(label, xy=(value - 47000, index - 0.10), color='red')


# cust_transact(pd_credit)
