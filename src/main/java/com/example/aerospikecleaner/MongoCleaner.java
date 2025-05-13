package com.example.aerospikecleaner;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * MongoCleaner is responsible for cleaning null values from MongoDB database records.
 * 
 * This class connects to a MongoDB database using configuration from application.properties,
 * queries records for null values in specific fields (he and hm), and updates those records
 * by removing the null values. It processes records in batches for better performance and
 * provides statistics about the cleaning process.
 * 
 * The class implements proper resource management with try-with-resources and provides
 * detailed logging of the cleaning process.
 */
public class MongoCleaner {

    // Constant for batch processing
    private static final int MAX_BATCH_SIZE = 500;

    // MongoDB client and collection
    private final MongoClient mongoClient;
    private final MongoCollection<Document> collection;

    // Statistics counters
    private int bothNullCount = 0;
    private int onlyHeNullCount = 0;
    private int onlyHmNullCount = 0;
    private int recordsWithEitherNull = 0;
    private int updatedCount = 0;

    /**
     * Default constructor that loads configuration from application.properties.
     * This constructor eliminates the need for hardcoded configuration values.
     * It initializes the MongoDB client and retrieves the specified database and collection.
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
        
        // Initialize MongoDB client and collection
        mongoClient = MongoClients.create(uri);
        MongoDatabase database = mongoClient.getDatabase(dbName);
        collection = database.getCollection(collectionName);
    }

    /**
     * Main method to clean null values from MongoDB records.
     * This method queries the MongoDB collection for records with null values
     * in he or hm fields and updates them by removing the null values.
     */
    public void clean() {
        System.out.println("\nStarting MongoDB cleaning process...");
        
        // Create a filter to find documents with null he or hm fields
        Bson filter = Filters.or(
            Filters.eq("he", null),
            Filters.eq("hm", null)
        );
        
        // Find all matching documents
        try (MongoCursor<Document> cursor = collection.find(filter).batchSize(MAX_BATCH_SIZE).iterator()) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                processDocument(doc);
            }
        }
        
        // Print statistics after cleaning
        System.out.println("\nMongoDB Cleaning Statistics:");
        System.out.println("Records with both he and hm null: " + bothNullCount);
        System.out.println("Records with only he null: " + onlyHeNullCount);
        System.out.println("Records with only hm null: " + onlyHmNullCount);
        System.out.println("Total records with either null: " + recordsWithEitherNull);
        System.out.println("Total records updated: " + updatedCount);
    }

    /**
     * Processes a MongoDB document, checking for null values in he and hm fields.
     * If null values are found, the document is updated to remove those values.
     * 
     * @param doc The MongoDB document to process
     */
    private void processDocument(Document doc) {
        Object heValue = doc.get("he");
        Object hmValue = doc.get("hm");
        
        boolean heIsNull = (heValue == null);
        boolean hmIsNull = (hmValue == null);
        
        if (!heIsNull && !hmIsNull) {
            return;
        }
        
        recordsWithEitherNull++;
        
        // Update statistics based on which fields are null
        if (heIsNull && hmIsNull) {
            bothNullCount++;
        } else if (heIsNull) {
            onlyHeNullCount++;
        } else {
            onlyHmNullCount++;
        }
        
        // Create update operations to unset null fields
        Bson updates = null;
        if (heIsNull && hmIsNull) {
            updates = Updates.combine(
                Updates.unset("he"),
                Updates.unset("hm")
            );
        } else if (heIsNull) {
            updates = Updates.unset("he");
        } else {
            updates = Updates.unset("hm");
        }
        
        // Update the document
        collection.updateOne(Filters.eq("_id", doc.getObjectId("_id")), updates);
        updatedCount++;
    }

    /**
     * Closes the MongoDB client to release resources.
     * This method should be called when the cleaner is no longer needed.
     */
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}