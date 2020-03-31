from random import choice
import random
import csv
import uuid


def get_fam_base(file_name):
    with open(file_name, "r", encoding='utf-8') as f:
        lis = [line.split() for line in f]
        data = []
        for i, x in enumerate(lis):
            data.append(x[0])
        data = data[1:]
    return data


def get_phone_number():
    prefix = ['910', '999', '495', '901']
    i = 0
    n = ''
    while i < 5:
        n += str(random.randint(0, 9))
        i += 1
    return choice(prefix) + n


def generate_data(fam, imy):
    loop = 100000
    i = 0
    result = []
    while i < loop:
        result.append(choice(fam) + ' ' + choice(imy) + ';' + get_phone_number())
        i += 1
    f = open('result.csv', 'w', newline='', encoding='utf-8')
    with f:
        fnames = ['fio', 'phone', 'links', 'uuid']
        writer = csv.DictWriter(f, fieldnames=fnames)
        writer.writeheader()
        for i in result:
            writer.writerow({'fio': i.split(';')[0], 'phone': i.split(';')[1],
                             'links': random.randint(1, 4), 'uuid': str(uuid.uuid4())})


if __name__ == "__main__":
    generate_data(get_fam_base('data/final_f'), get_fam_base('data/final_i.csv'))




