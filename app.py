import pandas as pd
from sqlalchemy import create_engine
import psycopg2

def main():
    src = create_engine('mysql+pymysql://root:password123@localhost/batest')
    dest = 'postgresql://postgres:samuel1997...@localhost:5432/Training'
    dest_conn = psycopg2.connect('postgresql://postgres:password123@localhost:5432/Training')
    table_names = ["batches", "Insufficients", "PrimeAirtimes", "Primeauths", "primedata", "SequelizeMeta", "Transactions", "Users"]
    writer = pd.ExcelWriter(r'C:\Users\OWOEYE TEMITOPE\Downloads\Online_Retail.xlsx\table_extracts.xlsx')
    dest_cursor = dest_conn.cursor()

    for table_name in table_names:
        query = f'select * from {table_name}'
        query2 = f'truncate table {table_name}'
        dest_cursor.execute(query2)
        dest_conn.commit()
        for chunk in pd.read_sql(query, con=src, chunksize=10):
            init_col_names = chunk.columns  # This contains a list of the initial column names
            conv_col_names = []
            for i in init_col_names:
                word_split = i.split('_')  # This split each word of the column names separated by underscore(_) and does nothing on any column name that is not separated by an underscore.
                camelcase = word_split[0] + ''.join(word.title() for word in word_split[1:])  # This is where the camelCasing is being achieved
                conv_col_names.append(camelcase)
            chunk.columns = conv_col_names
            chunk.to_sql(table_name, con=dest, if_exists='append', index=False, chunksize=10)

            df_from_pg = pd.read_sql(query, con=dest)

            try:
                df_from_pg.to_excel(writer, sheet_name=table_name, index=False)
                print(f'Data  in table {table_name}  Spooled to excel successfully')
            except:
                print('Data spooling failed')

if __name__  == "__main__":
    main()