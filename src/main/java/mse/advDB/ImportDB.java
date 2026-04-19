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

            while ((line = br.readLine()) != null && count < nbArticles) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                JsonObject article;
                try (JsonReader reader = Json.createReader(new StringReader(line))) {
                    article = reader.readObject();
                }

                final String articleId = article.getString("id", "");
                final String title = article.getString("title", "");

                if (articleId.isEmpty()) {
                    continue;
                }

                try (Session session = driver.session()) {

                    session.writeTransaction(tx -> {
                        tx.run(
                                "MERGE (a:ARTICLE {_id: $articleId}) "
                                + "SET a.title = $title",
                                parameters("articleId", articleId, "title", title)
                        );
                        return null;
                    });

                    JsonArray authors = article.getJsonArray("authors");
                    if (authors != null) {
                        for (int i = 0; i < authors.size(); i++) {
                            JsonObject author = authors.getJsonObject(i);
                            String rawId = author.getString("id", "");
                            final String authorId = rawId.isEmpty() ? "unknown_" + articleId + "_" + i : rawId;
                            final String authorName = author.getString("name", "");

                            session.writeTransaction(tx -> {
                                tx.run(
                                        "MERGE (au:AUTHOR {_id: $authorId}) "
                                        + "SET au.name = $name "
                                        + "WITH au "
                                        + "MATCH (a:ARTICLE {_id: $articleId}) "
                                        + "MERGE (au)-[:AUTHORED]->(a)",
                                        parameters("authorId", authorId, "name", authorName, "articleId", articleId)
                                );
                                return null;
                            });
                        }
                    }
                    JsonArray references = article.getJsonArray("references");
                    if (references != null) {
                        for (int i = 0; i < references.size(); i++) {
                            final String refId = ((JsonString) references.get(i)).getString();
                            if (refId.isEmpty()) {
                                continue;
                            }

                            session.writeTransaction(tx -> {
                                tx.run(
                                        "MERGE (ref:ARTICLE {_id: $refId}) "
                                        + "WITH ref "
                                        + "MATCH (a:ARTICLE {_id: $articleId}) "
                                        + "MERGE (a)-[:CITE]->(ref)",
                                        parameters("refId", refId, "articleId", articleId)
                                );
                                return null;
                            });
                        }
                    }
                }

                count++;
                if (count % 100 == 0) {
                    System.out.println("Imported " + count + " articles...");
                }
            }
        }

        System.out.println("Import complete");
        driver.close();
    }
}
