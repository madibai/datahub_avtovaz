import psycopg2
import pandas as pd
import numpy as np
import dask.dataframe as ds
from difflib import SequenceMatcher
import datetime
import Levenshtein

familiya_ = 3
phone_quantity = 100
levi_porog = 2


def get_conn():
    connection = psycopg2.connect(dbname='test_renault', user='postgres',
                                  password='password', host='localhost')
    return connection


def close_conn(conn):
    conn.close()


def get_data(conn):
    cursor = conn.cursor()
    cursor.execute('Select f_buyer, i_buyer, dv_customermail '
                   'from public.test_delivery '
                   'where dv_customermail_valid = 1 LIMIT 100')
    records = cursor.fetchmany(size=15)
    for i in records:
        print(i)

    cursor.close()


def get_email_intersect(conn):
    cursor = conn.cursor()
    cursor.execute('select dv_customermail from public.test_delivery '
                   'where dv_customermail_valid = 1 '
                   'intersect '
                   'select email from public.test_leads '
                   'where test_leads.email_valid = 1')
    records = cursor.fetchall()
    cursor.close()
    return records


def get_phone_intersect(conn):
    cursor = conn.cursor()
    cursor.execute('select mophone_buyer from public.test_delivery '
                   'intersect '
                   'select phone from public.test_leads')
    records = cursor.fetchall()
    cursor.close()
    return records


def compare_by_email(conn):
    cursor = conn.cursor()
    a = datetime.datetime.now()
    #   cursor.execute('select * from public.test_delivery ')
    sql1 = "select * from public.test_delivery;"
    delivery = pd.read_sql_query(sql1, conn)
    sql2 = "select * from public.test_leads;"
    leads = pd.read_sql_query(sql2, conn)
    print('select = ', datetime.datetime.now() - a)
    cursor.close()
    leads['f_fio'].fillna(' ', inplace=True)
    leads['i_fio'].fillna(' ', inplace=True)
    delivery['f_buyer'].fillna(' ', inplace=True)
    delivery['i_buyer'].fillna(' ', inplace=True)
    delivery.rename(columns={'dv_customermail': 'email'}, inplace=True)
    delivery['email'].fillna('-', inplace=True)
    leads['email'].fillna('-', inplace=True)

    leads['fi_l'] = leads['f_fio'] + ' ' + leads['i_fio']
    delivery['fi_d'] = delivery['f_buyer'] + ' ' + delivery['i_buyer']

    a = datetime.datetime.now()
    s1 = pd.merge(leads[(leads['email_valid'] == 1) & (leads['email'] != '-')],
                  delivery[(delivery['dv_customermail_valid'] == 1) & (delivery['email'] != '-')],
                  how='inner', on=['email'])
    print('merge s1 pd= ', datetime.datetime.now() - a)
    a = datetime.datetime.now()
    s1['f_base_match'] = s1.apply(lambda x: x['f_fio'] in x['fi_d'].split(), axis=1)
    print('f_base_match pd= ', datetime.datetime.now() - a)
    a = datetime.datetime.now()
    s1['i_base_match'] = s1.apply(lambda x: x['i_fio'] in x['fi_d'].split(), axis=1)
    print('i_base_match pd= ', datetime.datetime.now() - a)

    print(len(s1[s1['f_base_match'] == True]))
    print(len(s1[s1['i_base_match'] == True]))
    print(len(s1[(s1['i_base_match'] == True) & (s1['f_base_match'] == True)]))
    print(len(s1[(s1['i_base_match'] == True) | (s1['f_base_match'] == True)]))

    a = datetime.datetime.now()
    s2 = pd.merge(delivery[(delivery['dv_customermail_valid'] == 1) & (delivery['email'] != '-')],
                  leads[(leads['email_valid'] == 1) & (leads['email'] != '-')],
                  how='inner', on=['email'])
    print('merge s2 pd= ', datetime.datetime.now() - a)
    a = datetime.datetime.now()
    s2['f_base_match'] = s2.apply(lambda x: x['f_buyer'] in x['fi_l'].split(), axis=1)
    print('f_base_match pd= ', datetime.datetime.now() - a)
    a = datetime.datetime.now()
    s2['i_base_match'] = s2.apply(lambda x: x['i_buyer'] in x['fi_l'].split(), axis=1)
    print('i_base_match pd= ', datetime.datetime.now() - a)

    print(len(s2[s2['f_base_match'] == True]))
    print(len(s2[s2['i_base_match'] == True]))
    print(len(s2[(s2['i_base_match'] == True) & (s2['f_base_match'] == True)]))
    print(len(s2[(s2['i_base_match'] == True) | (s2['f_base_match'] == True)]))

    a = datetime.datetime.now()
    s2['f_levi'] = s2.apply(lambda x: f_levi(x), axis=1)
    print('f_levi delivery = ', datetime.datetime.now() - a)
    print(s2['f_levi'].value_counts())


def compare_by_phone(conn):
    cursor = conn.cursor()
    a = datetime.datetime.now()
    #   cursor.execute('select * from public.test_delivery ')
    sql1 = "select * from public.test_delivery;"
    delivery = pd.read_sql_query(sql1, conn)
    sql2 = "select * from public.test_leads;"

    leads = pd.read_sql_query(sql2, conn)
    #   print('select = ', datetime.datetime.now() - a)
    cursor.close()

    leads['phone'].fillna(' ', inplace=True)
    delivery['mophone_buyer'].fillna(' ', inplace=True)
    leads['f_fio'].fillna(' ', inplace=True)
    leads['i_fio'].fillna(' ', inplace=True)
    delivery['f_buyer'].fillna(' ', inplace=True)
    delivery['i_buyer'].fillna(' ', inplace=True)

    leads['fi_l'] = leads['f_fio'] + ' ' + leads['i_fio']
    delivery['fi_d'] = delivery['f_buyer'] + ' ' + delivery['i_buyer']

    leads['fi_l'].fillna('-', inplace=True)
    delivery['fi_d'].fillna('-', inplace=True)

    r1 = leads['phone'].value_counts().loc[lambda x: x > phone_quantity].index.tolist()
    r2 = delivery['mophone_buyer'].value_counts().loc[lambda x: x > phone_quantity].index.tolist()

    r1_lead = []
    for i in r1:
        if len(leads[leads['phone'] == i].groupby('f_fio').count()) > familiya_:
            r1_lead.append(i)
    r2_delivery = []
    for i in r2:
        if len(delivery[delivery['mophone_buyer'] == i].groupby('f_buyer').count()) > familiya_:
            r2_delivery.append(i)

    delivery_less_phone = delivery[~delivery.mophone_buyer.isin(r2_delivery)]
    lead_less_phone = leads[~leads.phone.isin(r1_lead)]

    common_phone = np.intersect1d(delivery_less_phone['mophone_buyer'].unique(),
                                  lead_less_phone['phone'].unique())
    full_lead_phone_common = lead_less_phone[lead_less_phone['phone'].isin(common_phone)]
    delivery_phone_common = delivery_less_phone[delivery_less_phone['mophone_buyer'].isin(common_phone)]
    delivery_phone_common.rename(columns={'mophone_buyer': 'phone'}, inplace=True)
    print('2', datetime.datetime.now())
    a = datetime.datetime.now()

    print(len(lead_less_phone))
    print(len(delivery_less_phone))

    delivery_less_phone.rename(columns={'mophone_buyer': 'phone'}, inplace=True)
    phone_temp_result = pd.merge(full_lead_phone_common,
                                 delivery_phone_common,
                                 how='left', on='phone')
    print('merge s1 pd= ', datetime.datetime.now() - a)

    a = datetime.datetime.now()
    phone_temp_result['f_base_match'] = phone_temp_result.apply(
        lambda x: x['f_fio'] in x['fi_d'].split(), axis=1)
    print('f_base_match pd= ', datetime.datetime.now() - a)

    a = datetime.datetime.now()
    phone_temp_result['i_base_match'] = phone_temp_result.apply(
        lambda x: x['i_fio'] in x['fi_d'].split(), axis=1)

    print('i_base_match pd= ', datetime.datetime.now() - a)
    print(len(phone_temp_result[phone_temp_result['f_base_match'] == True]))
    print(len(phone_temp_result[phone_temp_result['i_base_match'] == True]))
    print(len(phone_temp_result[(phone_temp_result['i_base_match'] == True)
                                & (phone_temp_result['f_base_match'] == True)]))
    print(len(phone_temp_result[(phone_temp_result['i_base_match'] == True)
                                | (phone_temp_result['f_base_match'] == True)]))

    a = datetime.datetime.now()
    phone_temp_result_delivery = pd.merge(delivery_phone_common,
                                          full_lead_phone_common,
                                          how='left', on='phone')
    print('merge s2 pd= ', datetime.datetime.now() - a)

    a = datetime.datetime.now()
    phone_temp_result_delivery['f_base_match'] = phone_temp_result_delivery.apply(
        lambda x: x['f_buyer'] in x['fi_l'].split(), axis=1)
    print('f_base_match pd= ', datetime.datetime.now() - a)
    a = datetime.datetime.now()
    phone_temp_result_delivery['i_base_match'] = phone_temp_result_delivery.apply(
        lambda x: x['i_buyer'] in x['fi_l'].split(), axis=1)
    print('i_base_match pd= ', datetime.datetime.now() - a)

    print(len(phone_temp_result_delivery[phone_temp_result_delivery['f_base_match'] == True]))
    print(len(phone_temp_result_delivery[phone_temp_result_delivery['i_base_match'] == True]))
    print(len(
        phone_temp_result_delivery[(phone_temp_result_delivery['i_base_match'] == True)
                                   & (phone_temp_result_delivery['f_base_match'] == True)]))
    print(len(
        phone_temp_result_delivery[(phone_temp_result_delivery['i_base_match'] == True)
                                   | (phone_temp_result_delivery['f_base_match'] == True)]))

    a = datetime.datetime.now()
    phone_temp_result['f_levi'] = phone_temp_result.apply(lambda x: f_levi(x), axis=1)
    print('f_levi lead = ', datetime.datetime.now() - a)
    print(phone_temp_result['f_levi'].value_counts())


def f_levi(x):
    try:
        if (x['f_base_match'] == False) and (len(x['f_buyer']) > 3 and len(x['f_fio']) > 3):
            if Levenshtein.distance(x['f_buyer'], x['f_fio']) < levi_porog:
                return True
            else:
                return False
        else:
            return False
    except:
        return False


if __name__ == '__main__':
    """
    new_conn = get_conn()
    get_data(new_conn)
    email_intersect = get_email_intersect(new_conn)
    print(len(email_intersect))
    phone_intersect = get_phone_intersect(new_conn)
    print(len(phone_intersect))
    close_conn(new_conn)
    """

    """
    seq = SequenceMatcher(None, 'иванов', 'ивалов')
    print(seq.ratio())
    print(seq.get_matching_blocks())

    print('==Levi==')
    print(Levenshtein.distance('иванов', 'ивалов'))
    print(Levenshtein.ratio('иванов', 'ивалов'))
    """
    new_conn = get_conn()
    compare_by_email(new_conn)
    compare_by_phone(new_conn)
    close_conn(new_conn)
