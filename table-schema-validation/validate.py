import pyodbc
from csv import DictReader
from csv import reader
import logging

logger = logging.getLogger(__name__)
logging.getLogger().setLevel(logging.INFO)


## create list of dictionaries from table data to comapare
def get_column_from_table():
    
    col_cursor = cnxn.cursor()
    table_col_cursor = col_cursor.execute(""" select COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='test' """)

    table_row_result= table_col_cursor.fetchall()
    #logging.info(table_row_result)
    
    #create a list which contains the key(column headers from table)
    keyList = ['COLUMN_NAME','DATA_TYPE','CHARACTER_MAXIMUM_LENGTH']
    table_val_compare_list = []
    
    for i, sourceRow in enumerate(table_row_result):
        
        table_row = (str(sourceRow[0]) + str(sourceRow[1]) + str(sourceRow[2]))
        targetRowObject ={}
        targetRowObject["compareKey"] = table_row
        table_val_compare_list.append(targetRowObject)
    #logging.info(table_val_compare_list)
    return table_val_compare_list



## create list of dictionaries from csv to comapare
def get_column_from_csv(file_path):
    
    #create a list which contains the key(column headers from csv file)
    csv_col_list =['Column','DataType','Data_Length']
    
    with open(file_path, 'r') as read_obj:
        csv_reader = reader(read_obj)
        header = next(csv_reader)
        if header != None:
            csv_val_compare_list = []
            
            for row_1 in csv_reader:
                row = (str(row_1[1]) + str(row_1[2]) + str(row_1[3]))
                #logging.info(row)
                
                targetRowObject = {}
                targetRowObject["compareKey"] = row
                csv_val_compare_list.append(targetRowObject)
            #logging.info(csv_val_compare_list)
            return csv_val_compare_list


if __name__ == "__main__":
    
    
    try:
        
        #Setting up connection to SQL Server 
        cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                        "Server=NINDER;"
                                        "Database=ninder;"
                                        "username = 'sa';"
                                        "password = 'kkk@8888';"
                                        "Trusted_Connection=yes;")
        
        #Query SQL Server using pyodbc cursor function
        cursor = cnxn.cursor()
        table_col_cursor = cursor.execute("select COLUMN_NAME,* from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME= 'test' ")
        
        
        #Getting data with headers
        columns = [column[0] for column in table_col_cursor.description]
        
        #intiating an empty list for row and column data
        global results
        results = []
        for row in table_col_cursor.fetchall():
            #logging.info(row)
            results.append(dict(zip(columns, row)))
            #logging.info(results)
        
        #Finding number of columns in the table using len function
        global number_of_columns_table
        number_of_columns_table = len(results)
        logging.info("Number of columns in table are {n}".format(n=number_of_columns_table))

        table_col_cursor.close()
        
        file_path ='D:/BHP/table/sample_data.csv'


        #open file in read mode
        with open(file_path, 'r') as read_obj:
            #pass the file object to DictReader() to get the DictReader object
            csv_dict_reader = DictReader(read_obj)
            
            global number_of_columns_csv
            number_of_columns_csv = len(list(csv_dict_reader))
            logging.info("Number of columns in csv file are {n}".format(n=number_of_columns_csv)) 
            
            if(number_of_columns_table != number_of_columns_csv):
                col_validation_result = 'Number of Columns are Mismatch in Table in CSV file'
                logging.info(col_validation_result)

                validation_comment = 'Both the Schema are Different as Number of Columns are Mismatch in Table in CSV file'
            
            elif(number_of_columns_table == number_of_columns_csv):
                col_validation_result = 'Number of Columns are equal, Check for col validation'
                logging.info(col_validation_result)
                
                
                logging.info("Fetching compare list from  get_column_from_csv method")
                csv_val_compare_list = get_column_from_csv(file_path)
                #logging.info(csv_val_compare_list)

                logging.info("Fetching compare list from  get_column_from_table method")
                table_val_compare_list = get_column_from_table()
                #logging.info(table_val_compare_list)

                pairs = zip(csv_val_compare_list, table_val_compare_list)
                is_lists_equal = str(any(x == y for x, y in pairs))
                if(is_lists_equal == 'True'):
                    validation_comment = "Both the Schema are Identical"

                elif(is_lists_equal == 'False'):
                    validation_comment = "Both the Schema are Different"

            
            logging.info(validation_comment)               


    except Exception as main_e:
        logging.info(main_e)
        logger.exception("Exception in main module")
        logger.exception(main_e)