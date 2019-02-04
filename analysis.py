import os
import numpy as np
import matplotlib.pyplot as plt
from mysql.connector import MySQLConnection, Error
import pandas as pd
from collections import Counter
from mysql_dbconfig import read_db_config

file = pd.read_csv('/home/blee/Downloads/output.csv')

# IPC
# take ipc class, drop empty class and split on the basis of '|'
def ipc_read():
    ipc = file['IPC(s)'].dropna().str.split('|')
    ipc_c = []

    for items in ipc:
            ipc_c += items

    ipc_combine = list(filter(lambda x: x != ' ', ipc_c))  # removing empty ipc class
    ipc_combines = [x.strip(' ') for x in ipc_combine]     # trimming spaces
    ipc_length = len(ipc_combines)
    # print(ipc_length)


    key_ipc = Counter(ipc_combine).keys()
    value_ipc = Counter(ipc_combine).values()
    recall_ipc = list('%.3f' % (i/ipc_length) for i in value_ipc)  # recall for ipc to 3 decimal point
    # print(recall_ipc)

    # list1 = pd.DataFrame({'code': key_ipc, 'count': value_ipc})
    tuple_ipc = list(zip(key_ipc, value_ipc))
    # print(data_tuple)
    # Storing the final code and respective count in data frame
    seed_ipc = pd.DataFrame(tuple_ipc, columns=['Code', 'Count']).head(30)
    print(seed_ipc.sort_values('Count', ascending=False)) # ranking done

    tuple_ipc_recall = list(zip(key_ipc,recall_ipc))
    dataframe_ipc_recall = pd.DataFrame(tuple_ipc_recall, columns=['ipc', 'recall']).head(30).sort_values('recall', ascending=False)  # ipc recall to data frame
    print(dataframe_ipc_recall)


ipc_read()


# CPC
def cpc_read():
    cpc = file['CPC(s)'].dropna().str.split('|')  # removing pipe(|) from a single CPC cell for a patent

    cpc_c = []

    for items in cpc:
        cpc_c += items    # combining all the cpc classes
    cpc_combine = [x.strip(' ') for x in cpc_c]  # trimming spaces

    cpc_length = len(cpc_combine)
    print(cpc_length)

    key_cpc = Counter(cpc_combine).keys()
    value_cpc = Counter(cpc_combine).values()

    recall_cpc = list('%.3f' % (i / cpc_length) for i in value_cpc)  # recall for cpc to 3 decimal point
    #print(recall_cpc)

    # list1 = pd.DataFrame({'code': key_cpc, 'count': value_cpc})
    tuple_cpc = list(zip(key_cpc, value_cpc))
    # print(data_tuple)
    # Storing the final code and respective count in data frame
    seed_cpc = pd.DataFrame(tuple_cpc, columns=['Code', 'Count']).head(30)
    print(seed_cpc.sort_values('Count', ascending=False)) # ranking done

    tuple_cpc_recall = list(zip(key_cpc, recall_cpc))
    dataframe_cpc_recall = pd.DataFrame(tuple_cpc_recall,  columns=['cpc', 'recall']).head(30).sort_values('recall',ascending=False)  # cpc recall to data frame
    print(dataframe_cpc_recall)


cpc_read()


# UPC
# def upc_read():
#     upc = file['UPC(s)'].dropna().str.split('|')
#
#     upc_c = []
#
#     for items in upc:
#         upc_c += items
#
#     upc_combine = [x.strip(' ') for x in upc_c]  # trimming spaces
#     print(upc_combine)
#     upc_length = len(upc_combine)
#     print(upc_length)
#
#     key_upc = Counter(upc_combine).keys()
#     value_upc = Counter(upc_combine).values()
#
#     recall_upc = list('%.3f' % (i / upc_length) for i in value_upc)  # recall for cpc to 3 decimal point
#     #print(recall_upc)
#
#     # list1 = pd.DataFrame({'code': key_upc, 'count': value_upc})
#     tuple_upc = list(zip(key_upc, value_upc))
#     print(tuple_upc)
#     # Storing the final code and respective count in data frame
#     seed_upc = pd.DataFrame(tuple_upc, columns=['Code', 'Count']).head(30).sort_values('Count', ascending=False)
#     print(seed_upc)
#
#     tuple_upc_recall = list(zip(key_upc, recall_upc))
#     dataframe_upc_recall = pd.DataFrame(tuple_upc_recall, columns=['upc', 'recall']).head(30).sort_values('recall',ascending=False)  # upc recall to data frame
#     print(dataframe_upc_recall)
#
# upc_read()


# dict = {'key': value}
#
# for key, value in dict.items():
#     print(dict[key])


def query_with_fetchall():
    dbconfig = read_db_config()
    conn = MySQLConnection(**dbconfig)
    cursor = conn.cursor()

    upc = file['UPC(s)'].dropna().str.split('|')

    upc_c = []

    for items in upc:
        upc_c += items

    upc_combine = [x.strip(' ') for x in upc_c]  # trimming spaces
    print(upc_combine)
    upc_length = len(upc_combine)
    print(upc_length)

    key_upc = Counter(upc_combine).keys()
    value_upc = Counter(upc_combine).values()
    print("Values")
    upc_check = list(key_upc)
    print(upc_check)

    recall_upc = list('%.6f' % (i / upc_length) for i in value_upc)  # recall for upc to 3 decimal point
    #print(recall_upc)

    # list1 = pd.DataFrame({'code': key_upc, 'count': value_upc})
    tuple_upc = list(zip(key_upc, value_upc))
    print(tuple_upc)
    # Storing the final code and respective count in data frame
    seed_upc = pd.DataFrame(tuple_upc, columns=['Code', 'Count']).sort_values('Count', ascending=False).head(30)
    print("SEEEDDDDDDDDDDDDDDDDDDDDDDDD")
    print(seed_upc)

    tuple_upc_recall = list(zip(key_upc, recall_upc))
    dataframe_upc_recall = pd.DataFrame(tuple_upc_recall, columns=['upc', 'recall']).sort_values('recall',ascending=False).head(30)  # upc recall to data frame
    print(dataframe_upc_recall)

    try:

        cursor.execute("SELECT * FROM uspc_class")
        rows = cursor.fetchall()

        upc_data = pd.DataFrame(rows, columns=['upc', 'total'])

        # new = upc_data['upc'].isin(upc_check)
        # print(upc_data.loc[upc_data['upc'].isin(upc_check)])

        new = upc_data.query("upc in @seed_upc.Code")  # filtering database equal to seed set
        print("NEWWWWWWWWWWWWWWWWWWWWWW")
        print(new)



        print('Total Row(s):', cursor.rowcount)
        upc_precision = {}   # precision calculate for upc

        for sindex, srow in seed_upc.iterrows():
            for dindex, drow in new.iterrows():
                if srow['Code'] == drow['upc']:
                    upc_precision.update({srow['Code']: ('%.6f' % (srow['Count'] / drow['total']))})

        names = list(upc_precision.keys())
        quant = list(upc_precision.values())

        tuple_upc_precision = list(zip(names,quant))
        upc_precision_dataframe = pd.DataFrame(tuple_upc_precision, columns=['upc', 'precision']).head(10)

        upc_precision_dataframe.upc = pd.to_numeric(upc_precision_dataframe.upc)
        upc_precision_dataframe.precision = pd.to_numeric(upc_precision_dataframe.precision)

        print("UPC PRECISION DATAFRAME")
        print(upc_precision_dataframe)
        mpr_upc = 1
        print(upc_precision_dataframe.info())
        # plt.bar(range(len(upc_precision)), quant, tick_label=names)
        # plt.savefig('bar.png')
        # plt.show()

        # index = np.arange(len(names))
        # plt.bar(index, quant)
        # plt.xlabel('Codes', fontsize=5)
        # plt.ylabel('Values', fontsize=5)
        # plt.xticks(index, names, fontsize=5, rotation=30)
        # plt.title('Precision Table')
        # plt.show()

        upc_precision_dataframe.plot.bar(x='upc', y='precision', rot=0)







    except Error as e:
        print(e)

    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    query_with_fetchall()