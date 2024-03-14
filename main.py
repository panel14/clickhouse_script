import datetime

from clickhouse_driver import Client
import enum
import random
import time

# Генерация для таблицы ages_grouped_by_flight_frequency
INSERTED_ROWS_COUNT = 1


class Location(enum.Enum):
    country = 1
    city = 2
    airport = 3


class Route(enum.Enum):
    location_from_id = 1
    location_to_id = 2


def connect_location(location):
    return location[Location.airport.value] + '(' + location[Location.city.value] + ',' + location[Location.country.value] + ')'


def create_route_string(route_id, routes, locations):
    route_tuple = routes[route_id]

    location_from_tuple = locations[route_tuple[Route.location_from_id.value] - 1]
    location_to_tuple = locations[route_tuple[Route.location_to_id.value] - 1]

    result_string = connect_location(location_from_tuple) + ' - ' + connect_location(location_to_tuple)
    return result_string


def str_time_prop(start, end, time_format, prop):

    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(time_format, time.localtime(ptime))


def random_date(start, end, prop):
    return str_time_prop(start, end, '%Y-%m-%d %H:%M:%S', prop)


def insert_ages_grouped_by_flight_frequency(r_strings, count):

    print('Creating data ages_grouped_by_flight_frequency')

    months = ["JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY", "AUGUST", "SEPTEMBER", "OCTOBER",
              "NOVEMBER", "DECEMBER"]

    table_data = [
        (random.randint(5, 55), random.choice(r_strings),
         random.choice(months),
         random.randint(1, 5))
        for _ in range(count)
    ]

    print('Data created. Executing insert to ages_grouped_by_flight_frequency')

    client.execute('insert into diploma.ages_grouped_by_flight_frequency (age, route, month, count) values',
                   table_data)

    print('Data inserted to ages_grouped_by_flight_frequency')


def insert_flights_humans_cost(r_strings, count):

    print('Creating data for flights_humans_cost')

    data = []

    for _ in range(count):
        date_start = random_date("2001-01-01 00:00:00", "2024-01-01 00:00:00", random.random())
        date_finish = datetime.datetime.strptime(date_start, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(
            minutes=random.randint(120, 600))

        route_str = random.choice(r_strings).split(' - ')

        data.append(
            (_ + 1, random.randint(1, 1000000), random.randint(2000, 50000),
             datetime.datetime.strptime(date_start, '%Y-%m-%d %H:%M:%S'), date_finish,
             random.randint(5, 55), route_str[0], route_str[1])
        )

    print('Data created. Executing insert to flights_humans_cost')

    client.execute('insert into diploma.flights_humans_cost (flight_id, human_id, cost, date_start,'
                   ' date_finish, age, place_to, place_from) values', data)

    print('Data inserted to flights_humans_cost')


def insert_flight_duration_options(r_strings, count):
    print('Creating data for flight_duration_options')

    option_types = ['Доплнительный багаж', 'Премиум класс', 'Двойная порция еды', 'Негабаритный багаж']

    data = []

    for _ in range(count):

        date_start = random_date("2001-01-01 00:00:00", "2024-01-01 00:00:00", random.random())
        date_start_formated = datetime.datetime.strptime(date_start, '%Y-%m-%d %H:%M:%S')

        delta = datetime.timedelta(minutes=random.randint(120, 600))

        date_finish = datetime.datetime.strptime(date_start, '%Y-%m-%d %H:%M:%S') + delta

        route_str = random.choice(r_strings).split(' - ')

        airport_str = route_str[0].split('(')
        city_str = airport_str[1].split(',')
        country_str = city_str[1].replace(')', '')

        airport_str_to = route_str[1].split('(')
        city_str_to = airport_str_to[1].split(',')
        country_str_to = city_str_to[1].replace(')', '')

        data.append((_ + 1, random.randint(1, 1000000), random.randint(1, 1000000),
                     delta.seconds / 3600, date_start_formated, date_finish, random.randint(5, 55),
                     random.choice(option_types), airport_str[0], city_str[0], country_str,
                     airport_str_to[0], city_str_to[0], country_str_to))

    print('Data created. Executing insert to flight_duration_options')

    client.execute('insert into diploma.flight_duration_options (flight_id, human_id, option_id, flight_time,'
                   ' date_start, date_finish, age, option_name, airport_from, city_from,'
                   ' country_from, airport_to, city_to, country_to) values', data)

    print('Data inserted to flight_duration_options')


def insert_profit_flights(r_strings, count):
    print('Creating data for profit_flights')

    r_count = len(r_strings)

    positions = ['Программист', 'Старший кадровик', 'Старший по отделу продаж',
                 'Инженер малых самолетов', 'Пилот', 'Бортпроводник']

    data = []

    for _ in range(count):

        date_start = random_date("2001-01-01 00:00:00", "2024-01-01 00:00:00", random.random())
        date_start_formated = datetime.datetime.strptime(date_start, '%Y-%m-%d %H:%M:%S')

        delta = datetime.timedelta(minutes=random.randint(120, 600))

        date_finish = datetime.datetime.strptime(date_start, '%Y-%m-%d %H:%M:%S') + delta

        r_id = random.randint(1, r_count)

        pos_id = random.randint(1, len(positions))

        data.append((_ + 1, random.randint(1, 1000000), r_id,
                     date_start_formated, date_finish, delta.seconds / 3600, r_strings[r_id - 1],
                     random.randint(1, 7), pos_id, positions[pos_id - 1], random.randint(60000, 150000)))

    print('Data created. Executing insert to flight_duration_options')

    client.execute('insert into diploma.profit_flights (flight_id, employee_id, route_id,'
                   ' date_start, date_finish, flight_time, route, work_time,'
                   ' position_id, position_name, employee_salary) values', data)

    print('Data inserted to profit_flights')


def insert_profitable_flights_routes(r_strings, count):
    print('Creating data for profitable_flights_routes')

    r_count = len(r_strings)

    planes_names = ['Малый самолет', 'Большой самолет', 'Аэробус']
    planes_capacity = [10, 20, 500]

    data = []

    for _ in range(count):
        r_id = random.randint(1, r_count)
        date_time = random_date("2001-01-01 00:00:00", "2024-01-01 00:00:00", random.random())
        date_time_formated = datetime.datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')

        plane_id = random.randint(0, 2)

        total_cost = float(random.randint(2000, 20000) * planes_capacity[plane_id])
        pure = total_cost * 0.4

        routes = r_strings[r_id - 1].split(' - ')

        data.append((_ + 1, r_id, date_time_formated, pure, routes[1], routes[0], total_cost,
                     total_cost * 1.3, planes_names[plane_id]))

    print('Data created. Executing insert to profitable_flights_routes')

    client.execute('insert into diploma.profitable_flights_routes (flight_id, route_id,'
                   ' date_time, clean_profit, place_to, place_from, total_cost,'
                   ' total_expense, plane_model) values', data)

    print('Data inserted to profitable_flights_routes')


# Подключаемся
client = Client(host='localhost', port=9000, user='clickhouse', password='password', database='diploma')

# Селектим таблицы локаций и маршрутов (из postgres через clickhouse)
locations_table = client.execute('SELECT * FROM postgresql(%(host)s, %(db)s, %(table)s, %(user)s, %(password)s)',
                                 {'host': 'postgres:5432', 'db': 'diploma', 'table': 'locations', 'user': 'postgres',
                                  'password': 'postgres'})

routes_table = client.execute('SELECT * FROM postgresql(%(host)s, %(db)s, %(table)s, %(user)s, %(password)s)',
                              {'host': 'postgres:5432', 'db': 'diploma', 'table': 'routes', 'user': 'postgres',
                               'password': 'postgres'})

routes_count = len(routes_table)


routes_strings = []

for i in range(routes_count):
    routes_strings.append(create_route_string(i, routes_table, locations_table))


insert_ages_grouped_by_flight_frequency(routes_strings, INSERTED_ROWS_COUNT)
insert_flights_humans_cost(routes_strings, INSERTED_ROWS_COUNT)
insert_flight_duration_options(routes_strings, INSERTED_ROWS_COUNT)
insert_profit_flights(routes_strings, INSERTED_ROWS_COUNT)
insert_profitable_flights_routes(routes_strings, INSERTED_ROWS_COUNT)

client.disconnect()




