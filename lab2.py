from threading import Thread
import time
import psycopg2

def create_table():
    conn = psycopg2.connect("host=localhost dbname=laba2 user=esmira password=Test#1234")
    cursor = conn.cursor()

    cursor.execute("CREATE TABLE user_counter (user_id serial PRIMARY KEY, counter integer, version integer);")
    cursor.execute("INSERT INTO user_counter (user_id, counter, version) VALUES (%s, %s, %s);", (1, 0, 0))
    cursor.execute("INSERT INTO user_counter (user_id, counter, version) VALUES (%s, %s, %s);", (2, 0, 0))
    cursor.execute("INSERT INTO user_counter (user_id, counter, version) VALUES (%s, %s, %s);", (3, 0, 0))
    cursor.execute("INSERT INTO user_counter (user_id, counter, version) VALUES (%s, %s, %s);", (4, 0, 0))

    conn.commit()
    conn.close()


def update_data():
    conn = psycopg2.connect("host=localhost dbname=laba2 user=esmira password=Test#1234")
    cursor = conn.cursor()
    
    cursor.execute('UPDATE user_counter SET counter = 0;')
    cursor.execute('UPDATE user_counter SET version = 0;')
    conn.commit()

    cursor.close()
    conn.close()


def lost_update():
    conn = psycopg2.connect("host=localhost dbname=laba2 user=esmira password=Test#1234")
    cursor = conn.cursor()
    uid = 1
    for _ in range(10000):
        query = u"SELECT counter FROM user_counter WHERE user_id = %s;"
        counter = cursor.execute(query, str(uid))
        counter = cursor.fetchone()[0]
        counter += 1
        cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s;", (counter, uid))
        conn.commit()
    cursor.close()
    conn.close()


def in_place_update():
    conn = psycopg2.connect("host=localhost dbname=laba2 user=esmira password=Test#1234")
    cursor = conn.cursor()
    uid = 2
    for _ in range(10000):
        query = u"UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s;"
        cursor.execute(query, str(uid))
        conn.commit()
    cursor.close()
    conn.close()


def row_level_locking():
    uid = 3
    for _ in range(10000):
        conn = psycopg2.connect("host=localhost dbname=laba2 user=esmira password=Test#1234")
        cursor = conn.cursor()
        query = u"SELECT counter FROM user_counter WHERE user_id = %s FOR UPDATE;"
        counter = cursor.execute(query, str(uid))
        counter = cursor.fetchone()[0]
        counter += 1
        cursor.execute(("UPDATE user_counter SET counter = %s WHERE user_id = %s;"), (counter, uid))
        conn.commit()
        cursor.close()
        conn.close()     


def optimistic_concurrency_locking():
    conn = psycopg2.connect("host=localhost dbname=laba2 user=esmira password=Test#1234")
    cursor = conn.cursor()
    uid = 4
    for _ in range(10000):
        while True:
            query = u"SELECT counter, version FROM user_counter WHERE user_id = %s;"
            cursor.execute(query, str(uid))
            counter, version = cursor.fetchone()
            counter += 1
            cursor.execute(("UPDATE user_counter SET counter = %s, version = %s WHERE user_id = %s and version = %s;"), (counter, version + 1, uid, version))
            conn.commit()
            count = cursor.rowcount
            if (count > 0):
                break
    cursor.close()
    conn.close()


def run_task(task_func, param):
    print("----------------","Task", param, "----------------")
    start_time = time.time()
    threads = [Thread(target=task_func, args=[]) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print("Time: %s seconds" % (time.time() - start_time))

    conn = psycopg2.connect("host=localhost dbname=laba2 user=esmira password=Test#1234")
    cursor = conn.cursor()
    cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s;", param)
    print(f'Counter value: {cursor.fetchone()[0]}')
    cursor.close()
    conn.close()


if __name__ == "__main__":
    # create_table()
    update_data()
    
    tasks = [
        (lost_update, "1"),
        (in_place_update, "2"),
        (row_level_locking, "3"),
        (optimistic_concurrency_locking, "4")
    ]

    for task_func, param in tasks:
        run_task(task_func, param)

    conn = psycopg2.connect("host=localhost dbname=laba2 user=esmira password=Test#1234")
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM user_counter ORDER BY user_id;')
    table = cursor.fetchall()

    print("\n")
    for row in table:
        print(f'user_id: {row[0]} counter: {row[1]} version: {row[2]}')

    cursor.close()
    conn.close()
    