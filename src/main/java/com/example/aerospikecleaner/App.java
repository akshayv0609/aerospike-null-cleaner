package com.example.aerospikecleaner;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class App {
    public static void main(String[] args) {
        System.out.println("Starting Aerospike and MongoDB null cleaner...");
        long startTime = System.currentTimeMillis();
        
        // Create an executor service with 2 threads
        ExecutorService executor = Executors.newFixedThreadPool(2);
        
        // Submit Aerospike cleaner task
        Future<CleanerResult> aerospikeResultFuture = executor.submit(() -> {
            try {
                System.out.println("Starting Aerospike cleaner in separate thread...");
                AerospikeCleaner aerospikeCleaner = new AerospikeCleaner();
                return aerospikeCleaner.cleanAndReturnResults();
            } catch (Exception e) {
                System.err.println("Error in Aerospike cleaner: " + e.getMessage());
                e.printStackTrace();
                throw e;
            }
        });
        
        // Submit MongoDB cleaner task
        Future<CleanerResult> mongoResultFuture = executor.submit(() -> {
            MongoCleaner mongoCleaner = null;
            try {
                System.out.println("Starting MongoDB cleaner in separate thread...");
                mongoCleaner = new MongoCleaner();
                return mongoCleaner.cleanAndReturnResults();
            } catch (Exception e) {
                System.err.println("Error in MongoDB cleaner: " + e.getMessage());
                e.printStackTrace();
                throw e;
            } finally {
                if (mongoCleaner != null) {
                    mongoCleaner.close();
                }
            }
        });
        
        // Shutdown the executor and wait for tasks to complete
        executor.shutdown();
        
        try {
            // Wait for both tasks to complete or timeout after 1 hour
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                System.err.println("Cleaners did not complete within the timeout period.");
                executor.shutdownNow();
                return;
            }
            
            // Get the results
            CleanerResult aerospikeResult = aerospikeResultFuture.get();
            CleanerResult mongoResult = mongoResultFuture.get();
            
            // Print results sequentially
            System.out.println("\n======================================");
            System.out.println("CLEANING PROCESS COMPLETE");
            System.out.println("======================================");
            
            aerospikeResult.printToConsole();
            mongoResult.printToConsole();
            
            long totalTime = System.currentTimeMillis() - startTime;
            System.out.println("\nTotal execution time: " + totalTime + " ms");
            System.out.println("Time saved by parallel execution: " + 
                (aerospikeResult.executionTimeMs + mongoResult.executionTimeMs - totalTime) + " ms");
            
        } catch (InterruptedException e) {
            System.err.println("Cleaning tasks were interrupted: " + e.getMessage());
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            System.err.println("Error executing cleaning tasks: " + e.getCause().getMessage());
            e.getCause().printStackTrace();
        }
    }
}