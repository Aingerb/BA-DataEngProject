
from process_sales_order_utils import retrieve_ingested_csv, create_fact_sales_order_dataframe, create_staff_dim_table, create_currency_dim_table, create_design_dim_table, create_counterparty_dim_table, create_date_dim_table, get_run_number, save_to_processed_sales_bucket, csvString_to_dict, create_location_dim_table
import pandas as pd
from datetime import datetime as dt

## use the functions written in the associated utils file to retrieve the most recent source data from the ingested bucket,
## process it into the desired format and then write it to the processed bucket in parquet form with the run number to act as a timestamp

def process_sales_schema():

    # find the run number of the most recent ingest using the run number tracker s3 bucket
    # run_num = get_run_number()
    run_num = 1004


    # retrieve the ingested CSV data from the Sales schema and save to variables representing each table
    sales_order_data = retrieve_ingested_csv('sales_order', run_num)
    staff_data = retrieve_ingested_csv('staff', run_num)
    dept_data = retrieve_ingested_csv('department', run_num)
    currency_data = retrieve_ingested_csv('currency', run_num)
    des_data = retrieve_ingested_csv('design', run_num)
    cp_data = retrieve_ingested_csv('counterparty', run_num)
    ad_data = retrieve_ingested_csv('address', run_num)

    # The counterparty data contains a company names column, some of the names have commas in them,
    # this causes issues with creating a dataframe with the correct columns. This section replaces the commas within the names with a placeholder
    temp_cp = [cp_row.split('"') for cp_row in cp_data.split('\n')]
    for row in temp_cp:
        if len(row) > 1:
            row[1] = row[1].replace(',', '<comma>')
    cp_data = str(temp_cp)[2:len(str(temp_cp))-1].replace('], [', '\n').replace(
        "', ", "").replace("'", "").replace('"', '').replace(']', '')


    # convert the csv data into dictionaries to allow accurate conversion to a Pandas dataframe. Replace any comma placeholders with commas
    sales_order_dict = csvString_to_dict(sales_order_data)
    dept_dict = csvString_to_dict(dept_data)
    staff_dict = csvString_to_dict(staff_data)
    currency_dict = csvString_to_dict(currency_data)
    des_dict = csvString_to_dict(des_data)
    cp_dict = csvString_to_dict(cp_data)
    ad_dict = csvString_to_dict(ad_data)

    # use the dictionaries to create Pandas dataframes
    sales_order_df = pd.DataFrame(sales_order_dict)
    dept_df = pd.DataFrame(dept_dict)
    staff_df = pd.DataFrame(staff_dict)
    currency_df = pd.DataFrame(currency_dict)
    design_df = pd.DataFrame(des_dict)
    counterparty_df = pd.DataFrame(cp_dict)
    address_df = pd.DataFrame(ad_dict)

    # call the relevent data processing functions
    fact_sales_order = create_fact_sales_order_dataframe(sales_order_df)
    staff_dim = create_staff_dim_table(staff_df, dept_df)
    currency_dim = create_currency_dim_table(currency_df)
    design_dim = create_design_dim_table(design_df)
    counterparty_dim = create_counterparty_dim_table(counterparty_df, address_df)
    date_dim = create_date_dim_table(sales_order_df)
    location_dim = create_location_dim_table(address_df)

    # save the resulting processed dataframes to the processed bucket in Parquet format
    df_list = [fact_sales_order, staff_dim, currency_dim,
               design_dim, counterparty_dim, date_dim, location_dim]
    table_names = ['sales', 'staff', 'currency',
                   'design', 'counterparty', 'date', 'location']

    
    # save each dataframe to the processed s3 bucket with the relevent file key to allow upload to the data warehouse
    for i in range(len(df_list)):
        save_to_processed_sales_bucket(table_names[i], run_num, df_list[i])

  

    #return the run number for logging
    return run_num

