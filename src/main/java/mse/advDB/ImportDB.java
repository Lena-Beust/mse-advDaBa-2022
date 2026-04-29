package mse.advDB;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import static org.neo4j.driver.Values.parameters;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonString;

import java.util.ArrayList;
import java.util.List;

public class ImportDB {

    public static void main(String[] args) throws IOException, InterruptedException {

        String jsonPath = System.getenv("JSON_FILE");
        System.out.println("Path to JSON file is " + jsonPath);

        int nbArticles = Integer.max(1000, Integer.parseInt(System.getenv("MAX_NODES")));
        System.out.println("Number of articles to consider is " + nbArticles);

        String neo4jIP = System.getenv("NEO4J_IP");
        System.out.println("IP address of neo4j server is " + neo4jIP);

        Driver driver = GraphDatabase.driver("bolt://" + neo4jIP + ":7687", AuthTokens.basic("neo4j", "test_neo4j"));

        // Attendre que Neo4j soit prêt
        boolean connected = false;
        do {
            try {
                System.out.println("Sleeping a bit waiting for the db");
                Thread.sleep(5000);
                driver.verifyConnectivity();
                connected = true;
            } catch (Exception e) {
                // retry
            }
        } while (!connected);

        System.out.println("Connected to Neo4j. Starting import...");

        try (Session session = driver.session()) {
            session.writeTransaction(tx -> {
                tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:ARTICLE) REQUIRE a._id IS UNIQUE");
                tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (au:AUTHOR) REQUIRE au._id IS UNIQUE");
                return null;
            });
        }

        try (BufferedReader br = new BufferedReader(new FileReader(jsonPath))) {
            String line;
            int count = 0;
            final int BATCH_SIZE = 1000;
            List<JsonObject> batch = new ArrayList<>();

            while ((line = br.readLine()) != null && count < nbArticles) {

                if (line.trim().isEmpty())
                    continue;

                JsonObject article;
                try (JsonReader reader = Json.createReader(new StringReader(line))) {
                    article = reader.readObject();
                }

                batch.add(article);

                if (batch.size() >= BATCH_SIZE) {
                    sendBatch(driver, batch);
                    batch.clear();
                }

                count++;

                if (count % 1000 == 0) {
                    System.out.println("Imported " + count);
                }
            }

            // dernier batch
            if (!batch.isEmpty()) {
                sendBatch(driver, batch);
            }
        }

        System.out.println("Import complete");
        driver.close();
    }

    private static void sendBatch(Driver driver, List<JsonObject> batch) {

        try (Session session = driver.session()) {

            session.writeTransaction(tx -> {

                tx.run("""
                            UNWIND $batch AS row

                            MERGE (a:ARTICLE {_id: row.id})
                            SET a.title = row.title

                            WITH a, row

                            UNWIND row.authors AS author
                            MERGE (au:AUTHOR {_id: author.id})
                            SET au.name = author.name
                            MERGE (au)-[:AUTHORED]->(a)

                            WITH a, row

                            UNWIND row.references AS refId
                            MERGE (ref:ARTICLE {_id: refId})
                            MERGE (a)-[:CITE]->(ref)
                        """, parameters("batch", batch));

                return null;
            });
        }
    }
}
