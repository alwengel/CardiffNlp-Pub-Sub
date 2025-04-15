import sqlite3
from pub_sub_data.cardifnlp_data.cardifnlp_datahandler import (
    get_cardifnlp_original_subscriptions,
    get_group_identifier,
    get_cardifnlp_subscriptions_expanded,
    replace_subscription_matches,
    load_dataset,
    binary_list_to_integer
)

DB_PATH = 'cardifnlp.db'

def init_db(conn):
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS publications (
            publication_id INTEGER PRIMARY KEY,
            publication TEXT
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS labels (
            label_id INTEGER PRIMARY KEY,
            label TEXT
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS subscriptions (
            subscription_id INTEGER PRIMARY KEY,
            label_id INTEGER,
            subscription TEXT,
            FOREIGN KEY(label_id) REFERENCES labels(id)
        );
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS publication_matches (
            publication_id INTEGER,
            label_id INTEGER,
            FOREIGN KEY(publication_id) REFERENCES publications(id),
            FOREIGN KEY(label_id) REFERENCES labels(id)
        );
    ''')
    
    conn.commit()


def insert_original_labels(conn):
    cursor = conn.cursor()
    for entry in get_cardifnlp_original_subscriptions():
        cursor.execute('''
            INSERT INTO labels (label_id, label) VALUES (?, ?)
        ''', (entry['identifier'], entry['subscription']))
    conn.commit()

def insert_subscriptions(conn):
    cursor = conn.cursor()

    for row in get_cardifnlp_subscriptions_expanded():
        group_id = get_group_identifier(row['subscription_id'])

        cursor.execute('''
            INSERT INTO subscriptions (subscription_id, label_id, subscription)
            VALUES (?, ?, ?)
        ''', (row['subscription_id'], group_id, row['subscription'], ))

    conn.commit()

def insert_publications(conn, number_of_publications):
    cursor = conn.cursor()
    publications = load_dataset()
    for pub in publications:

        pub_id = int(pub["id"])
        publication = pub["text"]

        cursor.execute('''
            INSERT INTO publications (publication_id, publication) VALUES (?, ?)
        ''', (pub_id, publication))

        label_ids = binary_list_to_values(pub['label'])

        for label_id in label_ids:
            cursor.execute('''
                INSERT INTO publication_matches (publication_id, label_id)
                VALUES (?, ?)
            ''', (pub_id, label_id))
    
    conn.commit()

def binary_list_to_values(identifier_list):
    return [2 ** i for i, bit in enumerate(reversed(identifier_list)) if bit == 1]

def save_schema_to_file(conn, filename="schema.sql"):
    cursor = conn.cursor()
    cursor.execute("SELECT sql FROM sqlite_master WHERE type='table';")
    schema_statements = cursor.fetchall()

    with open(filename, 'w') as f:
        for statement in schema_statements:
            if statement[0] is not None:
                f.write(statement[0] + ';\n\n')

    print(f"Schema saved to {filename}")


def build_database(conn):
    
    init_db(conn)
    insert_original_labels(conn)
    insert_subscriptions(conn)
    insert_publications(conn, 1000)
    save_schema_to_file(conn)
    conn.close()
    print("Database created successfully.")

if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    build_database(conn)
    
