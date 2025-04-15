CREATE TABLE publications (
            publication_id INTEGER PRIMARY KEY,
            publication TEXT
        );

CREATE TABLE labels (
            label_id INTEGER PRIMARY KEY,
            label TEXT
        );

CREATE TABLE subscriptions (
            subscription_id INTEGER PRIMARY KEY,
            label_id INTEGER,
            subscription TEXT,
            FOREIGN KEY(label_id) REFERENCES labels(id)
        );

CREATE TABLE publication_matches (
            publication_id INTEGER,
            label_id INTEGER,
            FOREIGN KEY(publication_id) REFERENCES publications(id),
            FOREIGN KEY(label_id) REFERENCES labels(id)
        );

