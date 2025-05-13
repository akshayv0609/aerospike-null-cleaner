package com.example.aerospikecleaner;

public class App {
    public static void main(String[] args) {
        // Run Aerospike Cleaner
        AerospikeCleaner aerospikeCleaner = new AerospikeCleaner();
        aerospikeCleaner.clean();  

        // Run MongoDB Cleaner
        MongoCleaner mongoCleaner = new MongoCleaner();
        try {
            mongoCleaner.runCleaner();
        } finally {
            mongoCleaner.close();
        }
    }
}