package com.example.aerospikecleaner;

/**
 * Main application class for the Aerospike and MongoDB Null Cleaner.
 * 
 * This class serves as the entry point for the application and orchestrates the execution
 * of both the Aerospike and MongoDB cleaning processes. It initializes both cleaners
 * with configurations loaded from application.properties and executes them in sequence.
 * 
 * The application first cleans null values from Aerospike records, then proceeds to clean
 * null values from MongoDB records. Each cleaner handles its own configuration loading,
 * connection management, and resource cleanup.
 * 
 * @author Akshay
 */
public class App {
    /**
     * Main method that serves as the entry point for the application.
     * Initializes and runs both the Aerospike and MongoDB cleaners in sequence.
     * 
     * @param args Command line arguments (not used in this application)
     */
    public static void main(String[] args) {
        // Run Aerospike Cleaner
        AerospikeCleaner aerospikeCleaner = new AerospikeCleaner();
        aerospikeCleaner.clean();  
        
        // Run MongoDB Cleaner
        MongoCleaner mongoCleaner = new MongoCleaner();
        try {
            mongoCleaner.clean();
        } finally {
            // Ensure MongoDB resources are properly closed
            mongoCleaner.close();
        }
    }
}