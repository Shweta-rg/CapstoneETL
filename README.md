# CapstoneETL

## CapstoneETL -Customer Credit-Card segments
# About The Project
# Background
•	One of the key pain points for a credit card is enact the payment system. It allows the cardholder to access financial services in exchange for the holder's promise to pay for them later.

•	The Credit Card System database is an independent system developed for managing activities such as registering new customers and approving or canceling requests, etc., using the architecture.
# Dataset
following datasets are used for this project

•	CDW_SAPP_CUSTOMER.JSON: This file has the existing customer details.

•	CDW_SAPP_CREDITCARD.JSON: contains all credit card transaction information.

•	CDW_SAPP_BRANCH.JSON: Each branch’s information and details are recorded in this file. 
 
 Dataset link:
 
 https://drive.google.com/drive/folders/1J4a2UndLvVWszHAL2VxJeVXyAHm3xYIp
 
 API link
 
 https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json


# Goals
•	Analyze and visualize credit card spending & paying data.

•	By understanding the customers, state which has higher transaction amount and launch a loan marketing campaign that is tailored for specific needs.

•	Understanding trend of how many loans application got approved based on employment and marital status. 
# Built With
•	Python, Pandas,  Seaborn, Matplotlib

•	PySpark 

•	Mysql (Heidi sql GUI)

•	Jupyter Notebook

# Procedures for ETL Project
1.	Import Python libraries 

2.	Extracting datasets- different JSON files and connecting with API endpoints

3.	Data cleaning

4.	Loading in mysql Database

5.	Data visualization and exploration

# Result: 
ETL Pipeline is successfully done by extracting, transformed and loaded in mySql database where data analyais and visualization was done by using panda and pyspark dataframes and matplotlib to plot different graphs.

# Findings
##  DAV:Branch with highest healthcare transactions
![14tranBybranch](https://user-images.githubusercontent.com/118309716/221956537-91b40300-e049-41ed-955e-0279735added.png)

#  DAV:top 3 months with the largest transaction data
![13monthstatus](https://user-images.githubusercontent.com/118309716/221956589-42684b09-d042-4313-918e-4b3d7ae3446d.png)

#  DAV:Percentage of rejected married male applicants
![12marriedapprov](https://user-images.githubusercontent.com/118309716/221957689-86f57437-0a2d-4834-af09-9cd098853ffe.png)

#  DAV:Percentage of applicatn approved  self-employ
![11selfemployedpng](https://user-images.githubusercontent.com/118309716/221956645-8b449c0a-4374-49f5-b57d-537249ee345c.png)
# DAV: Customer with highest transaction amount
![10new](https://user-images.githubusercontent.com/118309716/221956693-6a092e00-5934-46c8-b95a-5d4cb402d54c.png)
# DAV: State has a high number of customers
![9bystate](https://user-images.githubusercontent.com/118309716/221956748-d2575f45-396f-4a18-80db-d95be8f6656d.png)

# DAV: TransType has a high rate of trans
![8transbyTpe](https://user-images.githubusercontent.com/118309716/221957568-51df279e-db89-4772-a1b6-e3f44ec12836.png)













