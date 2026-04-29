from neo4j import GraphDatabase
import requests
import time

URI = "bolt://neo4j-service:7687"
USER = "neo4j"
PASSWORD = "password"

BATCH_SIZE = 1000

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

def fetch_data():
    # Exemple : remplace par ton API réelle
    for i in range(8_000_000):
        yield {"id": i, "name": f"item_{i}"}

def send_batch(tx, batch):
    query = """
    UNWIND $batch AS row
    MERGE (n:Item {id: row.id})
    SET n.name = row.name
    """
    tx.run(query, batch=batch)

def main():
    batch = []

    with driver.session() as session:
        for row in fetch_data():
            batch.append(row)

            if len(batch) >= BATCH_SIZE:
                session.execute_write(send_batch, batch)
                print(f"Inserted {len(batch)}")
                batch = []

        if batch:
            session.execute_write(send_batch, batch)

    driver.close()

if __name__ == "__main__":
    main()