from pyspark.sql.functions import *
import pyinputplus as pyip


def show_trans_type_val(df_sp_cc, tran_type):
    print("inside the function:transaction-type-", tran_type)
    data1 = df_sp_cc.select('transaction_type', 'transaction_value').\
        where(df_sp_cc['transaction_type'] == tran_type)\
        .agg(round(sum('transaction_value'), 2), count('transaction_type'))
    data1.show()


def test_tran_type_value(df_sp_cc, list_type):
    while True:
        # 2)    Used to display the number and total values of transactions for a given type.
        print("in while")
        # tran_type=pyip.inputStr("enter transaction type or 0 to exit:")
        tran_type = pyip.inputStr("Enter Transaction type (0 to Exit):")
        print("after input")
        # list_type = list(pd_credit['TRANSACTION_TYPE'].drop_duplicates())

        if tran_type in list_type:
            print("in the if cond :after validating")
            show_trans_type_val(df_sp_cc, tran_type)
        elif tran_type == '0':
            break

        else:
            print("enter valid type:")
            continue
