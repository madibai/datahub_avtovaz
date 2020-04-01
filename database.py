import psycopg2
import pandas as pd
import dask.dataframe as ds
from difflib import SequenceMatcher
import datetime
import Levenshtein


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


def compare_pd_dk(conn):
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

    leads['fi_l'] = leads['f_fio']+' '+leads['i_fio']
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
    compare_pd_dk(new_conn)
    close_conn(new_conn)
