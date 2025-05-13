package com.example.aerospikecleaner;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.io.InputStream;
import java.util.Properties;

public class AerospikeCleaner {
    private static final int MAX_BATCH_RECORDS = 500;

    private int bothNullCount = 0;
    private int onlyHeNullCount = 0;
    private int onlyHmNullCount = 0;
    private int recordsWithEitherNull = 0;
    private int nullHeOidCount = 0;
    private int nullHmOidCount = 0;

    private String host;
    private int port;
    private String namespace;
    private String setName;
    
    public AerospikeCleaner() {
        loadConfig();
    }
    
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
            setName = properties.getProperty("aerospike.set");
            
            if (host == null || portStr == null || namespace == null || setName == null) {
                System.err.println("Missing required Aerospike configuration in application.properties.");
                System.exit(1);
            }
            
            try {
                port = Integer.parseInt(portStr);
            } catch (NumberFormatException e) {
                System.err.println("Invalid Aerospike port in configuration: " + portStr);
                System.exit(1);
            }
        } catch (IOException ex) {
            System.err.println("Error loading Aerospike properties: " + ex.getMessage());
            System.exit(1);
        }
    }
    
    public void clean() {
        long startTime = System.currentTimeMillis();

        int totalRecords = 0;
        int updatedCount = 0;

        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.timeout = 2000;
        AerospikeClient client = new AerospikeClient(clientPolicy, host, port);
        WritePolicy policy = new WritePolicy();
        policy.sendKey = true;

        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(setName);

        RecordSet recordSet = client.query(null, stmt);
        List<RecordWithKey> batch = new ArrayList<>();

        try (BufferedWriter logWriter = new BufferedWriter(new FileWriter("invalid_pf_log.txt", true))) {
            while (recordSet.next()) {
                batch.add(new RecordWithKey(recordSet.getKey(), recordSet.getRecord()));

                if (batch.size() >= MAX_BATCH_RECORDS) {
                    updatedCount += processBatch(batch, client, policy, logWriter);
                    totalRecords += batch.size();
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                updatedCount += processBatch(batch, client, policy, logWriter);
                totalRecords += batch.size();
            }

            try (BufferedWriter statsWriter = new BufferedWriter(new FileWriter("statistics_log.txt", true))) {
                statsWriter.write("Total records processed: " + totalRecords + "\n");
                statsWriter.write("Records with both 'he' AND 'hm' null: " + bothNullCount + "\n");
                statsWriter.write("Records with only 'he' null: " + onlyHeNullCount + "\n");
                statsWriter.write("Records with only 'hm' null: " + onlyHmNullCount + "\n");
                statsWriter.write("Records with either 'he' OR 'hm' null: " + recordsWithEitherNull + "\n");
                statsWriter.write("Records updated: " + updatedCount + "\n");
                statsWriter.write("oid entries removed with type='hm' and null id: " + nullHmOidCount + "\n");
                statsWriter.write("oid entries removed with type='he' and null id: " + nullHeOidCount + "\n");
            }
        } catch (IOException e) {
            System.err.println("Log writing error: " + e.getMessage());
        } finally {
            recordSet.close();
            client.close();
            long endTime = System.currentTimeMillis();
            System.out.println("\nAerospike Cleaner Statistics:");
            System.out.println("Total records processed: " + totalRecords);
            System.out.println("Records with both 'he' AND 'hm' null: " + bothNullCount);
            System.out.println("Records with only 'he' null: " + onlyHeNullCount);
            System.out.println("Records with only 'hm' null: " + onlyHmNullCount);
            System.out.println("Records with either 'he' OR 'hm' null: " + recordsWithEitherNull);
            System.out.println("Records updated: " + updatedCount);
            System.out.println("oid entries removed with type='hm' and null id: " + nullHmOidCount);
            System.out.println("oid entries removed with type='he' and null id: " + nullHeOidCount);
            System.out.println("[Aerospike] Time taken: " + (endTime - startTime) + " ms");
        }
    }

    private int processBatch(List<RecordWithKey> batch, AerospikeClient client, WritePolicy policy, BufferedWriter logWriter) {
        int updated = 0;

        for (RecordWithKey rwk : batch) {
            Key key = rwk.key;
            Record record = rwk.record;

            try {
                if (record == null || !record.bins.containsKey("pf")) continue;
                Object pfBin = record.bins.get("pf");
                if (!(pfBin instanceof String)) continue;

                String pfStr = ((String) pfBin).trim();
                if (pfStr.isEmpty() || (!pfStr.startsWith("{") && !pfStr.startsWith("["))) continue;

                pfStr = unescapeJsonIfNeeded(pfStr);
                JSONObject pfJson;
                try {
                    pfJson = new JSONObject(pfStr);
                } catch (Exception e) {
                    pfStr = pfStr.replaceAll("\\\\\"", "\"").replaceAll("\\\\\\\\", "\\").replaceAll("\"null\"", "null");
                    try {
                        pfJson = new JSONObject(pfStr);
                    } catch (Exception e2) {
                        logWriter.write("Key: " + key.userKey + " | Error: " + e2.getMessage() + "\n");
                        continue;
                    }
                }

                boolean modified = false;
                boolean heIsNull = false, hmIsNull = false;

                if (pfJson.has("he")) {
                    Object heVal = pfJson.get("he");
                    if (heVal == null || "null".equalsIgnoreCase(heVal.toString().trim())) {
                        pfJson.remove("he");
                        heIsNull = true;
                        modified = true;
                    }
                }

                if (pfJson.has("hm")) {
                    Object hmVal = pfJson.get("hm");
                    if (hmVal == null || "null".equalsIgnoreCase(hmVal.toString().trim())) {
                        pfJson.remove("hm");
                        hmIsNull = true;
                        modified = true;
                    }
                }

                if (heIsNull && hmIsNull) {
                    bothNullCount++;
                    recordsWithEitherNull++;
                } else if (heIsNull) {
                    onlyHeNullCount++;
                    recordsWithEitherNull++;
                } else if (hmIsNull) {
                    onlyHmNullCount++;
                    recordsWithEitherNull++;
                }

                if (pfJson.has("oid")) {
                    try {
                        JSONArray oidArray = pfJson.getJSONArray("oid");
                        JSONArray filteredOidArray = new JSONArray();

                        for (int i = 0; i < oidArray.length(); i++) {
                            JSONObject obj = oidArray.getJSONObject(i);
                            String type = obj.optString("type");
                            Object idVal = obj.opt("id");
                            boolean isNullId = (idVal == null || "null".equalsIgnoreCase(idVal.toString().trim()));

                            if ("hm".equalsIgnoreCase(type) && isNullId) {
                                nullHmOidCount++;
                                modified = true;
                                continue;
                            }

                            if ("he".equalsIgnoreCase(type) && isNullId) {
                                nullHeOidCount++;
                                modified = true;
                                continue;
                            }

                            filteredOidArray.put(obj);
                        }
                        pfJson.put("oid", filteredOidArray);
                    } catch (Exception e) {
                        logWriter.write("Key: " + key.userKey + " | oid error: " + e.getMessage() + "\n");
                    }
                }

                if (modified) {
                    client.put(policy, key, new Bin("pf", pfJson.toString()));
                    updated++;
                    System.out.println("Updated: " + key.userKey);
                }
            } catch (Exception e) {
                System.err.println("Error for key: " + key.userKey + " - " + e.getMessage());
            }
        }

        return updated;
    }

    private String unescapeJsonIfNeeded(String pfStr) {
        if ((pfStr.startsWith("\"{") && pfStr.endsWith("}\"")) || (pfStr.startsWith("\"[") && pfStr.endsWith("]\""))) {
            pfStr = pfStr.substring(1, pfStr.length() - 1);
            pfStr = pfStr.replace("\\\"", "\"").replace("\\\\", "\\");
        }
        return pfStr;
    }

    private static class RecordWithKey {
        Key key;
        Record record;
        RecordWithKey(Key key, Record record) {
            this.key = key;
            this.record = record;
        }
    }
} 
