import sqlite3
import json

class DBHandler:
    def __init__(self, db_path):
        """
        Initializes the database handler with a path to the SQLite database file.
        """
        self.db_path = db_path
        self.connection = None
        self.cursor = None

    def connect(self):
        """
        Establishes a connection to the SQLite database.
        """
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row  # Enables dictionary-style row access
            self.cursor = self.connection.cursor()
            print("Database connection established.")
        except sqlite3.Error as e:
            print(f"An error occurred while connecting to the database: {e}")

    def close(self):
        """
        Closes the database connection.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Database connection closed.")

    def __enter__(self):
        """
        Allows the use of 'with' statements for managing the database connection.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Ensures the connection is closed after usage.
        """
        self.close()

    def get_subscriptions(self):
        """
        Fetches all subscriptions from the database and returns them as a list of dictionaries.
        Each dictionary contains 'subscription_id' and 'subscription'.
        """
        query = "SELECT subscription_id, subscription FROM subscriptions;"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return [dict(row) for row in rows]


    def get_random_publications(self, limit):
        """
        Retrieves a number of random publications with their matched labels.
        
        :param limit: Number of random publications to fetch.
        :return: List of dictionaries in the format:
            [
                {
                    "publication_id": <id>,
                    "publication": <text>,
                    "subscription_matches": [
                        {"subscription": <label>, "subscription_id": <id>},
                        ...
                    ]
                },
                ...
            ]
        """
        try:
            # Step 1: Get random publications
            self.cursor.execute("SELECT publication_id, publication FROM publications ORDER BY RANDOM() LIMIT ?", (limit,))
            publications = self.cursor.fetchall()

            result = []
            for pub in publications:
                publication_id = pub["publication_id"]
                publication_str = pub["publication"]

                # Step 2: Get matched labels from publication_matches
                self.cursor.execute("""
                    SELECT l.label_id AS subscription_id, l.label AS subscription
                    FROM publication_matches pm
                    JOIN labels l ON pm.label_id = l.label_id
                    WHERE pm.publication_id = ?
                """, (publication_id,))
                matches = [dict(row) for row in self.cursor.fetchall()]

                result.append({
                    "publication_id": publication_id,
                    "publication": publication_str,
                    "subscription_matches": matches
                })

            return result

        except sqlite3.Error as e:
            print(f"An error occurred while fetching publications: {e}")
            return []





def save_to_json(data, output_path):
    """
    Fetches a number of random publications from the DB and saves them to a JSON file.

    :param db_handler: An instance of DBHandler (connected).
    :param number_of_publications: Number of random publications to fetch.
    :param output_path: Path where the JSON file will be saved.
    """
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print(f"Saved {len(data)} publications to {output_path}")

if __name__=="__main__":

    DB_PATH = "cardifnlp.db"
    number_of_publications = 4200

    with DBHandler("cardifnlp.db") as db:
        subscriptions = db.get_subscriptions()
        for sub in subscriptions:
            print(sub)
