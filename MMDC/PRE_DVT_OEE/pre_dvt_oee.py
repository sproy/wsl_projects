import numpy as np
import pandas as pd
import os
from statistics import mean, median
from matplotlib import pyplot as plt

input_file_name = ('pre dvt 01.csv','pre dvt 02.csv')
result_file_name = 'analysis_result.csv'


def handle_raw_data(input_file_name):
    # read the raw data.
    df = pd.read_csv('./raw data/' + input_file_name)
    # df = pd.read_csv('./raw data/pre dvt 01.csv', index_col=['SFC','PLC_STATION_ID'])
    df[['DATE', 'TIME']] = df.START_DATE_TIME.str.split(' ', expand=True)
    # delete useless data.
    df.drop(['TIME', 'TXN_VARIANT_CODE', 'TXN_VARIANT_DATA', 'ACTIVITY',
             'CREATED_BY', 'CREATED_DATE_TIME', 'MODIFIED_DATE_TIME'], axis=1, inplace=True)
    # delete useless data - manually uploaded by developer
    df.drop(df[df.START_DATE_TIME == '01-JAN-0001 08:00:00'].index, inplace=True)
    # convert to datetime data type.
    df[['START_DATE_TIME', 'END_DATE_TIME']] = df[[
        'START_DATE_TIME', 'END_DATE_TIME']].apply(pd.to_datetime)

    return df


def cal_production_time(df):
    # df_start_max = df.groupby(['ORDER', 'DATE']).START_DATE_TIME.apply(lambda x: x.max() - x.min())
    df_start_min = df.groupby(
        ['ORDER', 'DATE']).START_DATE_TIME.apply(lambda x: x.min())
    df_end_max = df.groupby(
        ['ORDER', 'DATE']).END_DATE_TIME.apply(lambda x: x.max())
    df_combined = pd.concat([df_start_min, df_end_max],
                            axis=1, keys=['START', 'END'])
    df_combined['PRODUCTION_TIME'] = df_combined.END - df_combined.START
    with open(result_file_name,'a') as f:
        f.writelines(['-' * 30 + '\n', '[result for production time]:\n'])
    df_combined.to_csv(result_file_name, mode='a')
    print('-' * 30, f'[result for production time]:', sep='\n')
    print(df_combined)


def cal_downtime(df):
    df_order_station = get_order_station(df)
    result = list()
    for order, station in zip(df_order_station.ORDER, df_order_station.PLC_STATION_ID):
        df_good_sfc = df.loc[(df.PLC_STATION_ID == station)
                             & (df.ORDER == order)]
        df_result = df_good_sfc.groupby(['ORDER', 'DATE']).agg(
            {'END_DATE_TIME': lambda x: x.diff().nlargest(15).sum()})
        result.append(df_result)
    df_result = pd.concat(result, axis=0)
    with open(result_file_name,'a') as f:
        f.writelines(['-' * 30 + '\n', '[result for downtime]:\n'])
    df_result.to_csv(result_file_name, mode='a')
    print('-' * 30, f'[result for downtime]:', sep='\n')
    print(df_result)

def get_order_station(df):
    df_order_station = df[['PLC_STATION_ID']].groupby(df.ORDER).max()
    df_order_station.reset_index(inplace=True)
    return df_order_station


def cal_quality(df):
    df_order_station = get_order_station(df)
    result = list()
    for order, station in zip(df_order_station.ORDER, df_order_station.PLC_STATION_ID):
        df_good_sfc = df.loc[(df.PLC_STATION_ID == station)
                             & (df.ORDER == order) & (df.TXNTYPE == 1)]
        df_all_sfc = df.loc[(df.PLC_STATION_ID == 1)
                            & (df.ORDER == order)]
        count_good = df_good_sfc.groupby(['ORDER', 'DATE']).SFC.count()
        count_all = df_all_sfc.groupby(['ORDER', 'DATE']).SFC.count()
        df_temp = pd.concat([count_good, count_all],
                            axis=1, keys=['GOOD', 'ALL'])
        result.append(df_temp)
    df_result = pd.concat(result, axis=0)
    with open(result_file_name,'a') as f:
        f.writelines(['-' * 30 + '\n', '[result for quality data]:\n'])
    df_result.to_csv(result_file_name, mode='a')
    print('-' * 30, f'[result for quality data]:', sep='\n')
    print(df_result)


def cal_cycle_time(df):
    '''the maximum of end - start time determins the cycle time for that operation'''
    df['PROD_TIME'] = df.END_DATE_TIME - df.START_DATE_TIME
    df_group = df.groupby(['ORDER', 'DATE', 'PLC_STATION_ID'])
    # df_agg = df_group.PROD_TIME.apply(np.median)
    df_agg = df_group.PROD_TIME.describe()
    # df_agg.reset_index(level=2, inplace=True)
    df_agg.reset_index(level=2, inplace=True)
    with open(result_file_name,'a') as f:
        f.writelines(['-' * 30 + '\n', '[result for cycle time]:\n'])
    df_agg.to_csv(result_file_name, mode='a', header=True)
    print('-' * 30, f'[result for cycle time]:', sep='\n')
    print(df_agg)


def cal_lead_time(df):
    df_order_station = get_order_station(df)
    result = list()
    for order, station in (zip(df_order_station.ORDER, df_order_station.PLC_STATION_ID)):
        df_good_sfc = df[df.ORDER == order].groupby('SFC').filter(
            lambda x: x.PLC_STATION_ID.max() == station)
        df_temp = df_good_sfc.groupby(['ORDER', 'DATE', 'SFC']).agg(
            {'END_DATE_TIME': np.max, 'START_DATE_TIME': np.min})
        df_temp['LEAD_TIME'] = df_temp['END_DATE_TIME'] - \
            df_temp['START_DATE_TIME']
        df_result_order = df_temp.groupby(
            ['ORDER', 'DATE']).LEAD_TIME.describe()
        result.append(df_result_order)
    df_result = pd.concat(result, axis=0)
    with open(result_file_name,'a') as f:
        f.writelines(['-' * 30 + '\n', '[result for lead time]:\n'])
    df_result.to_csv(result_file_name, mode='a', header=True)
    print('-' * 30, f'[result for lead time]:', sep='\n')
    print(df_result)


def cal_fail(df):
    df_txn2 = df[df.TXNTYPE == 2]
    df_fail = df_txn2['TXNTYPE'].groupby(
        [df.LINE, df.PLC_STATION_ID, df.TXNTYPE])
    df_result = df_fail.count().rename('SCRAPPED')
    with open(result_file_name,'a') as f:
        f.writelines(['-' * 30 + '\n', '[result for failure data]:\n'])
    df_result.to_csv(result_file_name, mode='a', header=True)
    print('-' * 30, f'[result for failure data]:', sep='\n')
    print(df_result)


if __name__ == '__main__':
    # change the working directory.
    working_path = os.path.dirname(__file__)
    os.chdir(working_path)
    # clear the result file firstly.
    with open(result_file_name, 'r+') as f: f.truncate(0)
    for file in input_file_name:    
        with open(result_file_name,'a') as f:
            f.writelines([f'{"="*30}','\n', f'Analysing {file}, result is coming...\n', f'{"="*30}', '\n'])
        print(f'{"="*30}', f'Analysing {file}, result is coming...',f'{"="*30}', sep='\n')
        df = handle_raw_data(file)
        cal_production_time(df)
        cal_downtime(df)
        cal_cycle_time(df)
        cal_lead_time(df)
        cal_quality(df)
        cal_fail(df)
