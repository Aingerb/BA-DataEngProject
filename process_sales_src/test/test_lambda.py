import pytest
from moto import mock_s3, mock_iam, mock_lambda, mock_cloudwatch
import test_process_sales_order
import pandas as pd
from io import StringIO
import boto3
from create_process_sales_lambda import create_lambda


def add_ingested_to_mock_s3(s3):
    buckets = ['bosch-deploy-23-12-22-v2-ingest-bucket',
               'bosch-deploy-23-12-22-v2-processed-bucket',
               'bosch-deploy-23-12-22-v2-code-bucket']

    for bucket in buckets:
        s3.create_bucket(Bucket=bucket)

    # create run number tracker

    test_string = 'test run number'
    test_upload = StringIO(test_string)
    s3.put_object(Body=test_upload.getvalue(),
                  Bucket=buckets[0], Key='Run-tracker/run-number1.csv')

    # create the mock ingested data

    # sales order
    test_sales_order_headers = ['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id',
                                'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id']
    test_sales_order_rows = [1, '2022-11-03 14:20:52.186', '2022-11-03 14:20:52.186',
                             9, 16, 18, 84754, 2.43, 3, '2022-11-10', '2022-11-10', 4]
    test_sales_order_dict = {}
    for i in range(len(test_sales_order_headers)):
        test_sales_order_dict[test_sales_order_headers[i]] = [
            test_sales_order_rows[i]]
    test_df_sales = pd.DataFrame(test_sales_order_dict)

    # design
    test_design_headers = ['design_id', 'created_at', 'design_name', 'file_location', 'file_name',
                           'last_updated']
    test_design_rows = [['1', '2022-11-03 14:20:49.962', 'Wooden', '/home/user/dir',
                         'wooden-20201128-jdvi.json', '2022-11-03 14:20:49.962'],
                        ['3', '2022-11-03 14:20:49.962', 'Steel', '/usr/ports',
                         'steel-20210621-13gb.json', '2022-11-03 14:20:49.962']]
    test_design_dict = {}
    for i in range(len(test_design_headers)):
        test_design_dict[test_design_headers[i]] = [
            test_design_rows[0][i], test_design_rows[1][i]]
    test_design_df = pd.DataFrame(test_design_dict)

    # counterparty
    test_counterparty_headers = ['counterparty_id', 'counterparty_legal_name',
                                 'legal_address_id', 'commercial_contact', 'delivery_contact', 'created_at', 'last_updated']
    test_counterparty_rows = [[1, 'Fahey and Sons', 15, 'Micheal Toy', 'Mrs. Lucy Runolfsdottir', '2022-11-03 14:20:51.563', '2022-11-03 14:20:51.563'],
                              [2, "Leannon, Predovic and Morar", 28, 'Melba Sanford', 'Jean Hane III', '2022-11-03 14:20:51.563', '2022-11-03 14:20:51.563']]
    test_counterparty_dict = {}
    for i in range(len(test_counterparty_headers)):
        test_counterparty_dict[test_counterparty_headers[i]] = [
            test_counterparty_rows[0][i], test_counterparty_rows[1][i]]
    test_counterparty_df = pd.DataFrame(test_counterparty_dict)

    # address
    test_address_headers = ['address_id', 'address_line_1', 'address_line_2', 'district',
                            'city', 'postal_code', 'country', 'phone', 'created_at', 'last_updated']
    test_address_rows = [[15, '605 Haskell Trafficway', 'Axel Freeway', '', 'East Bobbie', '88253-4257', 'Heard Island and McDonald Islands', '9687 937447', '2022-11-03 14:20:49.962', '2022-11-03 14:20:49.962'],
                         [28, '079 Horacio Landing', '', '', 'Utica', '93045', 'Austria', '7772 084705', '2022-11-03 14:20:49.962', '2022-11-03 14:20:49.962']]
    test_address_dict = {}
    for i in range(len(test_address_headers)):
        test_address_dict[test_address_headers[i]] = [
            test_address_rows[0][i], test_address_rows[1][i]]
    test_address_df = pd.DataFrame(test_address_dict)

    # currency
    test_currency_dict = {'currency_id': [1, 2, 3], 'currency_code': [
        'GBP', 'USD', 'EUR'], 'created_at': [1, 2, 3], 'last_updated': [1, 2, 3]}
    test_currency_df = pd.DataFrame(test_currency_dict)

    # staff
    test_staff_headers = ['staff_id', 'first_name',
                          'last_name', 'email_address', 'department_id']
    test_staff_rows = [[1, 'Ben', 'Ainger', 'test@test.com', 1],
                       [2, 'Not Ben', 'Not Ainger', 'Secondtest@test.com', 1]]
    test_staff_dict = {}
    for i in range(len(test_staff_headers)):
        test_staff_dict[test_staff_headers[i]] = [
            test_staff_rows[0][i], test_staff_rows[1][i]]
    test_staff_df = pd.DataFrame(test_staff_dict)

    # department
    test_dept_df = pd.DataFrame({'department_id': [1], 'department_name': [
                                'testers'], 'location': ['remote']})

    df_list = [test_df_sales, test_address_df, test_counterparty_df,
               test_dept_df, test_staff_df, test_design_df, test_currency_df]
    name_list = ['sales_order', 'address', 'counterparty',
                 'department', 'staff', 'design', 'currency']
    # Load mock ingested data into mock bucket

    for i in range(len(df_list)):
        test_csv = StringIO()
        df_list[i].to_csv(test_csv)
        s3.put_object(Body=test_csv.getvalue(
        ), Bucket=buckets[0], Key=f'TableName/{name_list[i]}/RunNum:1.csv')


@mock_s3
def test_mock_s3_is_populated():

    s3 = boto3.client('s3')

    add_ingested_to_mock_s3(s3)

    test = s3.list_objects_v2(
        Bucket='bosch-deploy-23-12-22-v2-ingest-bucket')['Contents']
    print(test)

    assert len(test) == 8


@mock_s3
@mock_lambda
@mock_cloudwatch
@mock_iam
def test_lambda_is_created():

    s3 = boto3.client('s3')
    lambda_client = boto3.client('lambda')
    add_ingested_to_mock_s3(s3)

    create_lambda()
    lambdas = lambda_client.list_functions()['Functions']

    assert len(lambdas) == 1

