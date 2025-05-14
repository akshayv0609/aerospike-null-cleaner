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

/**
 * MongoCleaner is responsible for cleaning null values from MongoDB database records.
 * This class connects to a MongoDB database using configuration from application.properties,
 * finds records with null values in specific fields (he and hm), and updates those records
 * by removing the null values. It processes records in batches for better performance.
 */
public class MongoCleaner {

    private static final int MAX_BATCH_SIZE = 500;

    private final MongoClient mongoClient;
    private final MongoCollection<Document> collection;

    private int bothNullCount = 0;
    private int onlyHeNullCount = 0;
    private int onlyHmNullCount = 0;
    private int recordsWithEitherNull = 0;
    private int updatedCount = 0;

    /**
     * Default constructor that loads configuration from application.properties.
     * Initializes the MongoDB client and retrieves the specified database and collection.
     * Exits the application if any required configuration is missing or invalid.
     */
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

    /**
     * Cleans null values from MongoDB records and returns the results.
     * Finds records with null values, processes them in batches,
     * and collects statistics about the cleaning process.
     * 
     * @return CleanerResult containing statistics about the cleaning process
     */
    public CleanerResult cleanAndReturnResults() {
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

        long executionTime = System.currentTimeMillis() - startTime;
        
        return new CleanerResult(
            "MongoDB", 
            totalProcessed, 
            bothNullCount, 
            onlyHeNullCount, 
            onlyHmNullCount, 
            recordsWithEitherNull, 
            updatedCount, 
            0, // MongoDB doesn't track nullHeOidCount
            0, // MongoDB doesn't track nullHmOidCount
            executionTime
        );
    }

    /**
     * Cleans null values from MongoDB records and prints the results to the console.
     * This method is a wrapper around cleanAndReturnResults() that prints the results.
     */
    public void runCleaner() {
        CleanerResult result = cleanAndReturnResults();
        result.printToConsole();
    }

    /**
     * Processes a batch of documents, checking for null values and updating documents as needed.
     * 
     * @param batch List of documents to process
     */
    private void processBatch(List<Document> batch) {
        for (Document doc : batch) {
            //String id = doc.getObjectId("_id").toHexString();

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
            }
        }
    }

    /**
     * Checks if a field in a document is null or contains the string "null".
     * 
     * @param doc The document to check
     * @param fieldName The name of the field to check
     * @return true if the field is null or contains the string "null", false otherwise
     */
    private boolean isNullOrStringNull(Document doc, String fieldName) {
        return doc.containsKey(fieldName) && (
            doc.get(fieldName) == null ||
            "null".equalsIgnoreCase(doc.get(fieldName).toString().trim())
        );
    }

    /**
     * Closes the MongoDB client to release resources.
     * This method should be called when the cleaner is no longer needed.
     */
    public void close() {
        mongoClient.close();
    }
}