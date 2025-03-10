import psycopg2

try:
    conn = psycopg2.connect(
        dbname='airport',
        user='postgres',
        password='postgres',
        host='localhost',
        port='5432'
    )
    print('Соединение с БД установлено')
except Exception as err:
    conn=None
    print(f'Возникла ошибка {err}')

conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT version();")
version = cur.fetchmany(2)
print(version)

# with conn.cursor() as cur:
#     cur.execute("CREATE DATABASE airport")

create_table_airports = """
CREATE TABLE IF NOT EXISTS Airports(
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL
);
"""

create_table_passengers = """
CREATE TABLE IF NOT EXISTS Passengers(
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    passport_number VARCHAR(20) UNIQUE NOT NULL
);
"""

create_table_flights = """
CREATE TABLE IF NOT EXISTS Flights(
    id SERIAL PRIMARY KEY,
    departure_airport_id INT REFERENCES Airports(id),
    destination_airport_id INT REFERENCES Airports(id),
    departure_time TIMESTAMP NOT NULL,
    flight_number VARCHAR(20) NOT NULL    
);
"""

create_table_bookings = """
CREATE TABLE IF NOT EXISTS Bookings(
    id SERIAL PRIMARY KEY,
    flight_id INT REFERENCES Flights(id),
    passenger_id INT REFERENCES Passengers(id),
    booking_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL
);
"""

# with conn.cursor() as cur:
#     cur.execute(create_table_airports)
#     cur.execute(create_table_passengers)
#     cur.execute(create_table_flights)
#     cur.execute(create_table_bookings)

insert_data_airports = """
INSERT INTO Airports(name, city, country) VALUES
('Pulkovo', 'Saint-Petersburg', 'Russia'),
('Sheremetyevo', 'Moscow', 'Russia'),
('Domodedovo', 'Moscow', 'Russia'),
('Tolmachevo', 'Novosibirsk', 'Russia'),
('Kazan', 'Kazan', 'Russia') 
"""

insert_data_passengers = """
INSERT INTO Passengers (first_name, last_name, passport_number) VALUES
('Ivan', 'Ivanov', '123756'),
('Irina', 'Ivanova', '789123'),
('Petr', 'Petrov', '456789'),
('Sergey', 'Sidorov', '147258')
"""

insert_data_flights = """
INSERT INTO Flights (departure_airport_id, destination_airport_id, 
departure_time, flight_number) VALUES
(1, 2, '2025-11-02 10:00:00', 'SU258'),
(3, 4, '2025-11-01 12:00:00', 'SU125'),
(2, 3, '2025-11-05 14:30:00', 'SU652')
"""

insert_data_bookings = """
INSERT INTO Bookings (flight_id, passenger_id, status) VALUES
(1, 1, 'Confirmed'),
(1, 2, 'Pending Confirmation'),
(2, 3, 'Cancelled')
"""

# with conn.cursor() as cur:
#     cur.execute(insert_data_airports)
#     cur.execute(insert_data_passengers)
#     cur.execute(insert_data_flights)
#     cur.execute(insert_data_bookings)

query_1 = """
SELECT 
    Flights.flight_number,
    Passengers.first_name,
    Passengers.last_name,
    Bookings.status
FROM
    Bookings
JOIN
    Flights ON Bookings.flight_id = Flights.id
JOIN
    Passengers ON Bookings.passenger_id = Passengers.id;
"""
with conn.cursor() as cur:
    cur.execute(query_1)
    results = cur.fetchall()
print('1. Информация о рейсах и пассажирах:')
print(*results, sep='\n')

query_2 = """
SELECT
    Flights.flight_number,
    COUNT(Bookings.id) AS booking_count
FROM
    Bookings
JOIN
    Flights ON Bookings.flight_id = Flights.id
GROUP BY
    Flights.flight_number
HAVING
    COUNT(Bookings.id) > 1;
"""
with conn.cursor() as cur:
    cur.execute(query_2)
    results = cur.fetchall()
print('2. Количество бронирований по рейсам, рейсы с более чем одним бронированием:')
print(*results, sep='\n')

query_3 = """
SELECT
    first_name, last_name
FROM
    Passengers
ORDER BY
    last_name ASC;
"""
with conn.cursor() as cur:
    cur.execute(query_3)
    results = cur.fetchall()
print('3. Список пассажиров, отсортированный по фамилии:')
print(*results, sep='\n')

query_4 = """
SELECT
    *
FROM
    Flights
LIMIT 2;
"""
with conn.cursor() as cur:
    cur.execute(query_4)
    results = cur.fetchall()
print('4. Первые два рейса:')
print(*results, sep='\n')

query_5 = """
SELECT
    *
FROM
    Passengers
WHERE
    last_name LIKE 'I%';
"""
with conn.cursor() as cur:
    cur.execute(query_5)
    results = cur.fetchall()
print('5. Пассажиры с фамилией на букву И:')
print(*results, sep='\n')

conn.close()