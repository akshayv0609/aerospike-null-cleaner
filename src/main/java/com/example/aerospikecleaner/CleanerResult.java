package com.example.aerospikecleaner;

/**
 * CleanerResult class stores the results of a cleaning operation.
 * It contains statistics about the cleaning process and provides
 * methods to display these statistics.
 */
public class CleanerResult {
    public final String cleanerName;
    public final int totalRecordsProcessed;
    public final int bothNullCount;
    public final int onlyHeNullCount;
    public final int onlyHmNullCount;
    public final int recordsWithEitherNull;
    public final int updatedCount;
    public final int nullHeOidCount;
    public final int nullHmOidCount;
    public final long executionTimeMs;

    /**
     * Constructor for CleanerResult.
     * 
     * @param cleanerName Name of the cleaner (Aerospike or MongoDB)
     * @param totalRecordsProcessed Total number of records processed
     * @param bothNullCount Number of records with both 'he' and 'hm' null
     * @param onlyHeNullCount Number of records with only 'he' null
     * @param onlyHmNullCount Number of records with only 'hm' null
     * @param recordsWithEitherNull Number of records with either 'he' or 'hm' null
     * @param updatedCount Number of records updated
     * @param nullHeOidCount Number of oid entries removed with type='he' and null id
     * @param nullHmOidCount Number of oid entries removed with type='hm' and null id
     * @param executionTimeMs Execution time in milliseconds
     */
    public CleanerResult(String cleanerName, int totalRecordsProcessed, int bothNullCount, 
                         int onlyHeNullCount, int onlyHmNullCount, int recordsWithEitherNull, 
                         int updatedCount, int nullHeOidCount, int nullHmOidCount, 
                         long executionTimeMs) {
        this.cleanerName = cleanerName;
        this.totalRecordsProcessed = totalRecordsProcessed;
        this.bothNullCount = bothNullCount;
        this.onlyHeNullCount = onlyHeNullCount;
        this.onlyHmNullCount = onlyHmNullCount;
        this.recordsWithEitherNull = recordsWithEitherNull;
        this.updatedCount = updatedCount;
        this.nullHeOidCount = nullHeOidCount;
        this.nullHmOidCount = nullHmOidCount;
        this.executionTimeMs = executionTimeMs;
    }

    /**
     * Prints the cleaning statistics to the console.
     * Displays different statistics based on the cleaner type.
     */
    public void printToConsole() {
        System.out.println("\n" + cleanerName + " Cleaner Statistics:");
        System.out.println("Total records processed: " + totalRecordsProcessed);
        System.out.println("Records with both 'he' AND 'hm' null: " + bothNullCount);
        System.out.println("Records with only 'he' null: " + onlyHeNullCount);
        System.out.println("Records with only 'hm' null: " + onlyHmNullCount);
        System.out.println("Records with either 'he' OR 'hm' null: " + recordsWithEitherNull);
        System.out.println("Records updated: " + updatedCount);
        
        if (cleanerName.equals("Aerospike")) {
            System.out.println("oid entries removed with type='hm' and null id: " + nullHmOidCount);
            System.out.println("oid entries removed with type='he' and null id: " + nullHeOidCount);
        }
        
        System.out.println("Time taken: " + executionTimeMs + " ms");
    }
}