import pandas as pd
from sqlalchemy import create_engine
import psycopg2


def main():
    src = create_engine('mysql+pymysql://root:password@localhost/database_name')  # connection to the source database
    dest = 'postgresql://postgres:password@localhost:5432/database_name'   #Connection string from the destination database
    dest_conn = psycopg2.connect('postgresql://postgres:password@localhost:5432/database_name')  #Connection to the destination database
    table_names = ["batches", "insufficients", "primeairtimes", "primeauths", "primedata", "sequelizemeta", "transactions", "users"]
    writer = pd.ExcelWriter(r'C:\Users\OWOEYE TEMITOPE\Downloads\Online_Retail.xlsx\table_extracts.xlsx')  #This writes the data into an excel workbook
    dest_cursor = dest_conn.cursor()  #cursor for the connection to destination database.

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
    for table in table_names:
        query3 = f'select * from {table}'
        fin_df = pd.read_sql(query3, con=dest)

        print(fin_df)
        try:
            fin_df.to_excel(writer, sheet_name=table, index=False)
            writer.save()
            print('Data Spooled to excel successfully')
        except:
            print('Data spooling failed')
    writer.save()


if __name__ == "__main__":
    main()
