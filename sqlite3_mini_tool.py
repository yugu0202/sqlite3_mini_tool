from fire import Fire
import sqlite3

def create_table(db_name, table_name, *columns):
    columns_with_types = list(map(lambda x: [x[0],"string"] if len(x) == 1 else x,[c.split(":") for c in columns]))
    columns_text = ", ".join([f"{c[0]} {c[1]}" for c in columns_with_types])

    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute(f"CREATE TABLE {table_name} ({columns_text})")
    conn.commit()
    conn.close()

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