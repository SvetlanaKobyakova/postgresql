import psycopg2

try:
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='localhost',
        port='5432'
    )
    print(f'Соединение с БД установлено')
    #                           user       password  host     port  dbname
    #conn = psycopg2.connect("postgresql://postgres@localhost:5432/postgres")
except Exception as err:
    conn=None
    print(f'Возникла ошибка {err}')

# объект курсор для выполнения запросов
conn.autocommit = True #немедленное выполнение команды с изменением состояния БД
cur = conn.cursor()
cur.execute("SELECT version();")
version = cur.fetchmany(2)
print(version)

with conn.cursor() as cur:
    cur.execute("CREATE DATABASE library")