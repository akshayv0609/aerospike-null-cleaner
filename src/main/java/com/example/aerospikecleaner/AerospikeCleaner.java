package com.example.aerospikecleaner;

import com.aerospike.client.*;
import com.aerospike.client.policy.*;
import com.aerospike.client.query.*;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * AerospikeCleaner is responsible for cleaning null values from Aerospike database records.
 * 
 * This class connects to an Aerospike database using configuration from application.properties,
 * scans records for null values in specific fields (he and hm), and updates those records
 * by removing the null values. It processes records in batches for better performance and
 * provides statistics about the cleaning process.
 * 
 * The class implements proper resource management with try-with-resources and provides
 * detailed logging of the cleaning process.
 */
public class AerospikeCleaner {

    // Constants for batch processing and field names
    private static final int MAX_BATCH_SIZE = 500;
    private static final String HE_FIELD = "he";
    private static final String HM_FIELD = "hm";

    // Configuration properties
    private String host;
    private int port;
    private String namespace;
    private String set;

    // Statistics counters
    private int bothNullCount = 0;
    private int onlyHeNullCount = 0;
    private int onlyHmNullCount = 0;
    private int recordsWithEitherNull = 0;
    private int updatedCount = 0;

    /**
     * Default constructor that loads configuration from application.properties.
     * This constructor eliminates the need for hardcoded configuration values.
     */
    public AerospikeCleaner() {
        loadConfig();
    }

    /**
     * Loads configuration from application.properties file.
     * This approach allows for externalized configuration without hardcoding values.
     * The method validates that all required properties are present and exits if any are missing.
     */
    private void loadConfig() {
        Properties properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                System.err.println("Unable to find application.properties. This file is required for configuration.");
                System.exit(1);
            }
            
            properties.load(input);
            
            host = properties.getProperty("aerospike.host");
            String portStr = properties.getProperty("aerospike.port");
            namespace = properties.getProperty("aerospike.namespace");
            set = properties.getProperty("aerospike.set");
            
            if (host == null || portStr == null || namespace == null || set == null) {
                System.err.println("Missing required Aerospike configuration in application.properties.");
                System.exit(1);
            }
            
            try {
                port = Integer.parseInt(portStr);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number in application.properties: " + portStr);
                System.exit(1);
            }
        } catch (IOException ex) {
            System.err.println("Error loading Aerospike properties: " + ex.getMessage());
            System.exit(1);
        }
    }

    /**
     * Main method to clean null values from Aerospike records.
     * This method connects to the Aerospike database, scans records,
     * processes them in batches, and updates records with null values.
     */
    public void clean() {
        System.out.println("Starting Aerospike cleaning process...");
        
        try (AerospikeClient client = new AerospikeClient(host, port)) {
            // Create a statement for querying the database
            Statement stmt = new Statement();
            stmt.setNamespace(namespace);
            stmt.setSetName(set);
            
            // Create a record scanner
            try (RecordSet rs = client.query(null, stmt)) {
                List<Record> batch = new ArrayList<>();
                List<Key> batchKeys = new ArrayList<>();
                
                // Process records in batches
                while (rs.next()) {
                    Key key = rs.getKey();
                    Record record = rs.getRecord();
                    
                    batchKeys.add(key);
                    batch.add(record);
                    
                    if (batch.size() >= MAX_BATCH_SIZE) {
                        processBatch(client, batchKeys, batch);
                        batch.clear();
                        batchKeys.clear();
                    }
                }
                
                // Process any remaining records
                if (!batch.isEmpty()) {
                    processBatch(client, batchKeys, batch);
                }
            }
        }
        
        // Print statistics after cleaning
        System.out.println("\nAerospike Cleaning Statistics:");
        System.out.println("Records with both he and hm null: " + bothNullCount);
        System.out.println("Records with only he null: " + onlyHeNullCount);
        System.out.println("Records with only hm null: " + onlyHmNullCount);
        System.out.println("Total records with either null: " + recordsWithEitherNull);
        System.out.println("Total records updated: " + updatedCount);
    }

    /**
     * Processes a batch of records, checking for null values and updating records as needed.
     * This method improves performance by processing records in batches rather than individually.
     * 
     * @param client The Aerospike client
     * @param keys List of keys for the batch
     * @param records List of records for the batch
     */
    private void processBatch(AerospikeClient client, List<Key> keys, List<Record> records) {
        try (PrintWriter invalidProfileWriter = new PrintWriter(new FileWriter("invalid_pf_log.txt", true));
             PrintWriter statisticsWriter = new PrintWriter(new FileWriter("statistics_log.txt", true))) {
            
            for (int i = 0; i < records.size(); i++) {
                Record record = records.get(i);
                Key key = keys.get(i);
                
                // Process the record and update if needed
                processRecord(client, key, record, invalidProfileWriter);
            }
            
            // Write statistics to log file
            statisticsWriter.println("Batch processed. Current statistics:");
            statisticsWriter.println("Records with both he and hm null: " + bothNullCount);
            statisticsWriter.println("Records with only he null: " + onlyHeNullCount);
            statisticsWriter.println("Records with only hm null: " + onlyHmNullCount);
            statisticsWriter.println("Total records with either null: " + recordsWithEitherNull);
            statisticsWriter.println("Total records updated: " + updatedCount);
            statisticsWriter.println("------------------------------");
        } catch (IOException e) {
            System.err.println("Error writing to log files: " + e.getMessage());
        }
    }

    /**
     * Processes an individual record, checking for null values in he and hm fields.
     * If null values are found, the record is updated to remove those values.
     * 
     * @param client The Aerospike client
     * @param key The key of the record
     * @param record The record to process
     * @param invalidProfileWriter Writer for logging invalid profiles
     */
    private void processRecord(AerospikeClient client, Key key, Record record, PrintWriter invalidProfileWriter) {
        if (record == null) {
            return;
        }
        
        Object heObj = record.getValue(HE_FIELD);
        Object hmObj = record.getValue(HM_FIELD);
        
        boolean heIsNull = isNullValue(heObj);
        boolean hmIsNull = isNullValue(hmObj);
        
        // Skip if neither field is null
        if (!heIsNull && !hmIsNull) {
            return;
        }
        
        recordsWithEitherNull++;
        
        // Log the invalid profile
        String userKey = key.userKey != null ? key.userKey.toString() : key.digest.toString();
        invalidProfileWriter.println("Invalid profile found: " + userKey);
        
        // Update statistics based on which fields are null
        if (heIsNull && hmIsNull) {
            bothNullCount++;
            invalidProfileWriter.println("Both he and hm are null");
        } else if (heIsNull) {
            onlyHeNullCount++;
            invalidProfileWriter.println("Only he is null");
        } else {
            onlyHmNullCount++;
            invalidProfileWriter.println("Only hm is null");
        }
        
        // Create a JSON object from the record for updating
        JSONObject jsonObj = new JSONObject();
        for (String binName : record.bins.keySet()) {
            Object value = record.getValue(binName);
            if (!(binName.equals(HE_FIELD) && heIsNull) && !(binName.equals(HM_FIELD) && hmIsNull)) {
                jsonObj.put(binName, value);
            }
        }
        
        // Update the record to remove null values
        try {
            updateRecord(client, key, jsonObj);
            updatedCount++;
            invalidProfileWriter.println("Record updated successfully");
        } catch (Exception e) {
            invalidProfileWriter.println("Failed to update record: " + e.getMessage());
        }
        
        invalidProfileWriter.println("------------------------------");
    }

    /**
     * Updates a record in the Aerospike database with the provided JSON data.
     * 
     * @param client The Aerospike client
     * @param key The key of the record to update
     * @param jsonObj The JSON object containing the updated data
     */
    private void updateRecord(AerospikeClient client, Key key, JSONObject jsonObj) {
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.REPLACE;
        
        // Convert JSON object to Aerospike bins
        List<Bin> bins = new ArrayList<>();
        for (String binName : jsonObj.keySet()) {
            Object value = jsonObj.get(binName);
            bins.add(new Bin(binName, Value.get(value)));
        }
        
        // Update the record
        client.put(writePolicy, key, bins.toArray(new Bin[0]));
    }

    /**
     * Checks if a value is null or represents a null value in Aerospike.
     * 
     * @param value The value to check
     * @return true if the value is null or represents a null value, false otherwise
     */
    private boolean isNullValue(Object value) {
        return value == null || "null".equals(value) || "NULL".equals(value);
    }
}