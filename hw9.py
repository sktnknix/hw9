import csv
import re

from pymongo import MongoClient, errors
from datetime import datetime


def db_connection():
    try:
        return MongoClient('mongodb://mob.avenir-s.ru:27017') ### conn string выкинута на внешку
    except errors.ConnectionFailure as e:
        print('Failed connection to: %s' % e)


def read_data(csv_file, db):
    date_format = '%d/%m/%Y'
    database = db_connection()[db]
    db_collection = database.artists
    with open(csv_file, encoding='utf8') as csvfile:
        reader = csv.DictReader(csvfile)
        for line in reader:
            document = {}
            for keys in line:
                if keys == 'Дата':
                    document[keys] = datetime.strptime(line[keys].replace('.', '/') + '/2019',
                                                    date_format)
                else:
                    document[keys] = line[keys]
            db_collection.insert_one(document)
    print('Collection has been created ..')


def find_cheapest(db):
    database = db_connection()[db]
    db_collection = database.artists
    ascend_result = db_collection.find().sort("Цена")
    for line in ascend_result:
        print((line['Исполнитель'] + ' | ' + str(line['Цена']) + ' | ' +
               line['Место'] + ' | ' + f"{line['Дата']:%d-%m-%Y}"))


def find_by_name(name, db):
    database = db_connection()[db]
    db_collection = database.artists
    for line in db_collection.find().sort("Цена"):
        if re.search(name, line['Исполнитель'], re.I):
            print((line['Исполнитель'] + ' | ' + str(line['Цена']) + ' | ' +
                   line['Место'] + ' | ' + f"{line['Дата']:%d-%m-%Y}"))


def find_by_date_rate(db): ### ищем февральских
    database = db_connection()[db]
    db_collection = database.artists
    date_result = db_collection.find({"Дата": {'$gte': datetime(2019, 2, 1, 0, 0),
                                 '$lt': datetime(2019, 2, 27, 23, 59)}})
    for doc in date_result:
        print((doc['Исполнитель'] + ' | ' + str(doc['Цена']) + ' | ' +
               doc['Место'] + ' | ' + f"{doc['Дата']:%d-%m-%Y}"))

               
if __name__ == '__main__':
#   read_data('artists.csv', 'zzz')
#   find_cheapest('zzz')
#   find_by_name('Th', 'zzz')
    find_by_date_rate('zzz')
