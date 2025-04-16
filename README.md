# CardiffNlp-Pub-Sub
The CardiffNlp dataset of topic-labeled social media posts has  been repurposed to function as a dataset for evaluating a Pub-Sub system.

## Instructions
- Use **db_handler.py** to access cardiffnlp.db. Here you can find methods for extracting x number of randomly selected publications and save them to json in the following format:
```
[
  {
    "publication_id": <id>,
    "publication": <publication>,
    "subscription_matches": [
      {
        "subscription_id": <id>,
        "subscription": <subscription>
      },
      // more subscriptions matching the publication
    ]
  },
  // more publications
]
```
- See file **schema.sql** to understand the schema of cardiffnlp.db
