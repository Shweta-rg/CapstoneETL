# CapstoneETL

## CapstoneETL -Customer Credit-Card segments
# About The Project
# Background
•	One of the key pain points for a credit card is enact the payment system. It allows the cardholder to access financial services in exchange for the holder's promise to pay for them later.

•	The Credit Card System database is an independent system developed for managing activities such as registering new customers and approving or canceling requests, etc., using the architecture.
# Dataset
Following datasets are used for this project

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
•	Python, Pandas,  Seaborn, Matplotlib,plotly Express

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
used the menu driver user interface through which user can interact and get the results. there is a separate submenu for analysis and findings
![mainmenu](https://user-images.githubusercontent.com/118309716/222527007-d9680cee-861b-4509-98be-701c5d3384b1.png)

submenu for Data Analysis And Visulizations
![davmenu](https://user-images.githubusercontent.com/118309716/222527753-adc89b14-a35e-45ba-a6b0-b99b86ff5fc8.png)


# Findings

## DAV: State has a high number of customers

![sunburst-state](https://user-images.githubusercontent.com/118309716/222509730-2e6f4870-295d-4f62-ba37-1af725ec70b3.png)
 
 This sunburst grpah shows NY has high number oftransaction values from all the states.their purchasing power is way higher than other states
##  DAV:Branch with highest healthcare transactions
![14tranBybranch](https://user-images.githubusercontent.com/118309716/221956537-91b40300-e049-41ed-955e-0279735added.png)

##  DAV:top 3 months with the largest transaction data
![13monthstatus](https://user-images.githubusercontent.com/118309716/221956589-42684b09-d042-4313-918e-4b3d7ae3446d.png)

##  DAV:Percentage of rejected married male applicants
![12marriedapprov](https://user-images.githubusercontent.com/118309716/221957689-86f57437-0a2d-4834-af09-9cd098853ffe.png)

##  DAV:Percentage of applicatn approved  self-employ
![11selfemployedpng](https://user-images.githubusercontent.com/118309716/221956645-8b449c0a-4374-49f5-b57d-537249ee345c.png)
## DAV: Customer with highest transaction amount
![Screenshot 2023-03-02 111502](https://user-images.githubusercontent.com/118309716/222489495-c25be83a-0f0d-45da-ba11-c121baa8a458.png)

Thats hovering scatter plot which shows the customer with high transaction value and its valuable customer for bank.


## DAV: TransType has a high rate of trans
![8transbyTpe](https://user-images.githubusercontent.com/118309716/221957568-51df279e-db89-4772-a1b6-e3f44ec12836.png)













