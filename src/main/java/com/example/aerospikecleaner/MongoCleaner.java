package com.example.aerospikecleaner;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MongoCleaner {

    private static final int MAX_BATCH_SIZE = 500;

    private final MongoClient mongoClient;
    private final MongoCollection<Document> collection;

    private int bothNullCount = 0;
    private int onlyHeNullCount = 0;
    private int onlyHmNullCount = 0;
    private int recordsWithEitherNull = 0;
    private int updatedCount = 0;

    public MongoCleaner() {
    Properties properties = new Properties();
    String uri = null;
    String dbName = null;
    String collectionName = null;
    
    try (InputStream input = getClass().getClassLoader().getResourceAsStream("application.properties")) {
        if (input == null) {
            System.err.println("Unable to find application.properties. This file is required for configuration.");
            System.exit(1);
        }
        
        properties.load(input);
        
        uri = properties.getProperty("mongodb.uri");
        dbName = properties.getProperty("mongodb.database");
        collectionName = properties.getProperty("mongodb.collection");
        
        if (uri == null || dbName == null || collectionName == null) {
            System.err.println("Missing required MongoDB configuration in application.properties.");
            System.exit(1);
        }
    } catch (IOException ex) {
        System.err.println("Error loading MongoDB properties: " + ex.getMessage());
        System.exit(1);
    }
    
    mongoClient = MongoClients.create(uri);
    MongoDatabase database = mongoClient.getDatabase(dbName);
    collection = database.getCollection(collectionName);
}


    public void runCleaner() {
        long startTime = System.currentTimeMillis();

        int totalProcessed = 0;
        FindIterable<Document> documents = collection.find();
        List<Document> batch = new ArrayList<>();

        for (Document doc : documents) {
            batch.add(doc);
            if (batch.size() >= MAX_BATCH_SIZE) {
                processBatch(batch);
                totalProcessed += batch.size();
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            processBatch(batch);
            totalProcessed += batch.size();
        }

        long endTime = System.currentTimeMillis();

        System.out.println("\nMongoDB Cleaner Statistics:");
        System.out.println("Total records processed: " + totalProcessed);
        System.out.println("Records with both 'he' AND 'hm' null: " + bothNullCount);
        System.out.println("Records with only 'he' null: " + onlyHeNullCount);
        System.out.println("Records with only 'hm' null: " + onlyHmNullCount);
        System.out.println("Records with either 'he' OR 'hm' null: " + recordsWithEitherNull);
        System.out.println("Records updated: " + updatedCount);
        System.out.println("Processing complete. Time taken: " + (endTime - startTime) + " ms\n");
    }

    private void processBatch(List<Document> batch) {
        for (Document doc : batch) {
            String id = doc.getObjectId("_id").toHexString();

            boolean heNull = isNullOrStringNull(doc, "he");
            boolean hmNull = isNullOrStringNull(doc , "hm");
            boolean modified = false;
            List<Bson> updates = new ArrayList<>();

            if (heNull) {
                updates.add(Updates.unset("he"));
                modified = true;
            }
            if (hmNull) {
                updates.add(Updates.unset("hm"));
                modified = true;
            }

            if (heNull && hmNull) {
                bothNullCount++;
                recordsWithEitherNull++;
            } else if (heNull) {
                onlyHeNullCount++;
                recordsWithEitherNull++;
            } else if (hmNull) {
                onlyHmNullCount++;
                recordsWithEitherNull++;
            }

            if (modified) {
                collection.updateOne(Filters.eq("_id", doc.getObjectId("_id")), Updates.combine(updates));
                updatedCount++;
                System.out.println("Updated MongoDB record: " + id);
            }
        }
    }

    private boolean isNullOrStringNull(Document doc, String fieldName) {
        return doc.containsKey(fieldName) && (
            doc.get(fieldName) == null ||
            "null".equalsIgnoreCase(doc.get(fieldName).toString().trim())
        );
    }

    public void close() {
        mongoClient.close();
    }
}
