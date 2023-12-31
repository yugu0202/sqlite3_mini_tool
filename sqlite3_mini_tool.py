from fire import Fire
import sqlite3
import json

def create_table(db_name, table_name, *columns):
    columns_with_types = list(map(lambda x: [x[0],"string"] if len(x) == 1 else x,[c.split(":") for c in columns]))
    columns_text = ", ".join([f"{c[0]} {c[1]}" for c in columns_with_types])

    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute(f"CREATE TABLE {table_name} ({columns_text})")
    conn.commit()
    conn.close()

def insert_into_table(db_name, table_name, *values):
    values_text = ", ".join([f"'{v}'" for v in values])

    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute(f"INSERT INTO {table_name} VALUES ({values_text})")
    conn.commit()
    conn.close()

def delete_table(db_name, table_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute(f"DROP TABLE {table_name}")
    conn.commit()
    conn.close()

def create_tables_from_file(db_name, file):
    with open(file) as f:
        lines = [l.strip() for l in f.readlines()]
    for line in lines:
        if line.startswith("CREATE TABLE"):
            table_name = line.split(" ")[2]
            columns = line.split("(")[1].split(")")[0].split(",")
            create_table(db_name, table_name, *columns)
        elif line.startswith("INSERT INTO"):
            table_name = line.split(" ")[2]
            values = line.split("(")[1].split(")")[0].split(",")
            insert_into_table(db_name, table_name, *values)

def create_from_csv(db_name, table_name, csv_file):
    with open(csv_file) as f:
        columns = f.readline().strip().split(",")
        create_table(db_name, table_name, *columns)
        map(lambda x: insert_into_table(db_name, table_name, *x.strip().split(",")), f.readlines())

def create_from_json(db_name, table_name, json_file):
    with open(json_file) as f:
        columns = list(json.load(f)[0].keys())
        create_table(db_name, table_name, *columns)
        map(lambda x: insert_into_table(db_name, table_name, *x.values()), json.load(f))

def view_tables(db_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print(", ".join([" ".join(f) for f in c.fetchall()]))
    conn.close()

def view_schema(db_name, table_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute(f"PRAGMA table_info({table_name})")
    print("|".join([" ".join(list(map(lambda x: str(x),f))) for f in c.fetchall()]))
    conn.close()

def view_contents(db_name, table_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute(f"SELECT * FROM {table_name}")
    print("\n".join(["|".join(list(map(lambda x: str(x),f))) for f in c.fetchall()]))
    conn.close()

Fire()