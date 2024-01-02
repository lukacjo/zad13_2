import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def execute_sql(conn, sql):
    """Execute sql
    :param conn: Connection object
    :param sql: a SQL script
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)


def add_cook(conn, cook):
    """
    :param conn:
    :param cook:
    :return: cook id
    """
    sql = """INSERT INTO cooks(role_id, imie, nazwisko, data_urodzenia)
             VALUES(?,?,?,?)"""
    cur = conn.cursor()
    cur.execute(sql, cook)
    return cur.lastrowid


def add_role(conn, role):
    """
    Create a new task into the tasks table
    :param conn:
    :param role:
    :return: role id
    """
    try:
        sql = """INSERT INTO roles(nazwa, opis)
                VALUES(?,?)"""
        cur = conn.cursor()
        cur.execute(sql, role)
        conn.commit()
    except sqlite3.IntegrityError:
        print("Nie mogą być dwie role o tej samej nazwie!")
    return cur.lastrowid


def select_role_by_name(conn, name):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param role_nazwa:
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM roles WHERE nazwa=?", (name,))

    rows = cur.fetchall()
    print(rows)
    return rows


def select_all(conn, table):
    """
    Query all rows in the table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()
    print(rows)
    return rows


def select_where(conn, table, **query):
    """
    Query tasks from table with data from **query dict
    :param conn: the Connection object
    :param table: table name
    :param query: dict of attributes and values
    :return:
    """
    cur = conn.cursor()
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)
    cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
    rows = cur.fetchall()
    print(rows)
    return rows


def update(conn, table, id, **kwargs):
    """
    update status, begin_date, and end date of a task
    :param conn:
    :param table: table name
    :param id: row id
    :return:
    """
    parameters = [f"{k} = ?" for k in kwargs]
    parameters = ", ".join(parameters)
    values = tuple(v for v in kwargs.values())
    values += (id,)

    sql = f""" UPDATE {table}
             SET {parameters}
             WHERE id = ?"""
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print("Updated")
    except sqlite3.OperationalError as e:
        print(e)


def delete_where(conn, table, **kwargs):
    """
    Delete from table where attributes from
    :param conn:  Connection to the SQLite database
    :param table: table name
    :param kwargs: dict of attributes and values
    :return:
    """
    qs = []
    values = tuple()
    for k, v in kwargs.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)

    sql = f"DELETE FROM {table} WHERE {q}"
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    print(f"Deleted {values[0]} from {table}")


def delete_all(conn, table):
    """
    Delete all rows from table
    :param conn: Connection to the SQLite database
    :param table: table name
    :return:
    """
    sql = f"DELETE FROM {table}"
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    print(f"Deleted {table}")


def main():
    create_role_sql = """
            -- roles table
            CREATE TABLE IF NOT EXISTS roles (
                id INTEGER PRIMARY KEY,
                nazwa VARCHAR(250) NOT NULL UNIQUE,
                opis TEXT
            );
            """

    create_cooks_sql = """
            -- cooks table
            CREATE TABLE IF NOT EXISTS cooks (
                id INTEGER PRIMARY KEY,
                imie TEXT NOT NULL,
                nazwisko TEXT NOT NULL,
                data_urodzenia TEXT NOT NULL,
                role_id INTEGER NOT NULL,
                FOREIGN KEY (role_id) REFERENCES roles (id)
            );
            """

    db_file = "restaurant.db"

    conn = create_connection(db_file)

    if conn is not None:
        execute_sql(conn, create_role_sql)
        execute_sql(conn, create_cooks_sql)

        role = ("Head Chef", "Jeden nad wszystkimi")
        add_role(conn, role)

        role = ("Sous Chef", "Drugi nad wszystkimi")
        add_role(conn, role)

        role = ("Noob", "Nowy ziomek na zimnej")
        add_role(conn, role)

        cook = (1, "Jan", "Jankowski", "1990-05-13")
        add_cook(conn, cook)

        cook = (2, "Michał", "Czeski", "1990-08-13")
        add_cook(conn, cook)

        cook = (3, "Stanisław", "Wielki", "1999-12-21")
        add_cook(conn, cook)

        select_role_by_name(conn, "Head Chef")

        select_all(conn, "roles")
        select_all(conn, "cooks")

        select_where(conn, "cooks", nazwisko="Wielki")
        update(conn, "cooks", 5, imie="Alfred")
        select_where(conn, "cooks", nazwisko="Wielki")
        delete_where(conn, "cooks", imie="Stanisław")
        delete_all(conn, "cooks")

        conn.commit()
        conn.close()


if __name__ == "__main__":
    main()
