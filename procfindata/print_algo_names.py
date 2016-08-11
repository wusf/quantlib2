import os

filenames = os.listdir("financial_item_algos_by_sql")
for name in filenames:
    if name[-1]!='c':
        print name[:-3]