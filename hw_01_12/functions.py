import vaex
import json
import hashlib
import csv

file_path = "stocks_archive_5years.zip"
sort_columns = "high"
limit = 30
group_by_name = False
order = 'AAL'

date_sec = "2017-08-08"
name_sec = "PCLN"
filename = 'dump'
filename2 = 'dump_2.csv'
filename_type = '.csv'

## декоратор для функций, который считает количество вызовов функции (для соответствующего имени файла)
def decor_count(func):
    def wrapper(*args, **kwargs):
        wrapper.counter += 1
        return func(*args, **kwargs)
    wrapper.counter = 0
    return wrapper

## функция кэширования запроса
def cash_function(cashed_arguements, amount_of_calling):
    try:
        with open('cash.json', 'r') as my_favourite_json:
            data = json.load(my_favourite_json)
    except:
        data = {}
    try: 
        ## ищем запрос в кэшэ запросов
        return data[cashed_arguements]
    except: 
        ## записываем запрос в кэш запросов
        data[cashed_arguements] = amount_of_calling
        with open('cash.json', 'w') as my_favourite_json:
            json.dump(data, my_favourite_json)
        return 0

## Функция выполнения сортировки и фильтра
def select_sorted_backdoor(sort_columns, limit, order):
    dv = vaex.from_csv(file_path, convert = True)
    dv = dv[dv.Name == order]
    dv = dv.sort(sort_columns, ascending = False)
    dv  = dv[0:limit]
    return dv

@decor_count
def select_sorted(sort_columns_f = sort_columns , limit_f = limit, group_by_name_f = group_by_name, order_f = order):
    a = ['select_sorted_backdoor', sort_columns_f, str(limit_f), str(group_by_name_f), order_f]
    a = ','.join(a)
    cash_fu = cash_function(a, select_sorted.counter)
    if cash_fu == 0:
        ## Вызов функции впервые. Вызываем, обрабатываем, кэшируем результат
        b = select_sorted_backdoor(sort_columns_f, limit_f, order_f)
        b.export_hdf5(str(select_sorted.counter) + '.hdf5')
        b.export_csv(filename + str(select_sorted.counter) + filename_type)
    else:
        ## Функцию не вызываем, достаем инфу из кэша
        b = vaex.open(str(cash_fu) + '.hdf5')
        b.export_csv(filename + str(select_sorted.counter) + filename_type)


def get_by_date(date = date_sec, name=name_sec):
    ## одна из функций домашней работы
    dv = vaex.from_csv(file_path, convert=True)
    dvv = dv[dv.Name == name]
    dvv = dvv[dv.date == date]
    dvv.export_csv(filename + filename_type)

## Функция к домашке по 25 дню
def get_by_date2(date = date_sec, name = name_sec):

    a = []
    with open('all_stocks_5yr.csv', "r") as file:
        reader = csv.reader(file)
        print(reader)
        for i in reader:
            if i[0] == date and i[6] == name:
                a.append(i)

    with open(filename2, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(a)
