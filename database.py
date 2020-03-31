import psycopg2
import pandas as pd
#    import dask.dataframe as ds
from difflib import SequenceMatcher
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
    seq = SequenceMatcher(None, 'иванов', 'ивалов')
    print(seq.ratio())
    print(seq.get_matching_blocks())

    print('==Levi==')
    print(Levenshtein.distance('иванов', 'ивалов'))
    print(Levenshtein.ratio('иванов', 'ивалов'))

