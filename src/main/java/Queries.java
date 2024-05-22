import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class Queries {
    public static void executeWithNoIndex(String query, Connection conn) {
        try {
            long startTime = System.currentTimeMillis();
            conn.setAutoCommit(false);
            conn.createStatement().execute(query);
            conn.commit();
            long endTime = System.currentTimeMillis();
            System.out.println("Time taken to execute query with no index is "+(endTime - startTime) + " ms");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static void executeWithBTreeIndex(String query , HashMap<String,String> hashMap, Connection conn) {
        try {
            conn.setAutoCommit(false);
            conn.createStatement().execute("SET enable_seqscan = off");
            for (Map.Entry<String, String> entry : hashMap.entrySet()) {
                conn.createStatement().execute("CREATE INDEX " + entry.getKey() + "_" + entry.getValue() + "_index ON " + entry.getValue() + " USING BTREE(" + entry.getKey() + ");");
            }
            long startTime = System.currentTimeMillis();
            conn.createStatement().execute(query);
            long endTime = System.currentTimeMillis();
            for (Map.Entry<String, String> entry : hashMap.entrySet()) {
                conn.createStatement().execute("DROP INDEX " + entry.getKey() + "_" + entry.getValue() + "_index;");
            }
            conn.createStatement().execute("SET enable_seqscan = on");
            conn.commit();
            System.out.println("Time taken to execute query with BTree index is " + (endTime - startTime) + " ms");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static void executeWithHashIndex(String query , HashMap<String,String> hashMap, Connection conn) {
        try {
            conn.setAutoCommit(false);
            conn.createStatement().execute("SET enable_seqscan = off");
            for (Map.Entry<String, String> entry : hashMap.entrySet()) {
                conn.createStatement().execute("CREATE INDEX " + entry.getKey() + "_" + entry.getValue() + "_index ON " + entry.getValue() + " USING HASH(" + entry.getKey() + ");");
            }
            long startTime = System.currentTimeMillis();
            conn.createStatement().execute(query);
            long endTime = System.currentTimeMillis();
            for (Map.Entry<String, String> entry : hashMap.entrySet()) {
                conn.createStatement().execute("DROP INDEX " + entry.getKey() + "_" + entry.getValue() + "_index;");
            }
            conn.createStatement().execute("SET enable_seqscan = on");
            conn.commit();
            System.out.println("Time taken to execute query with Hash index is " + (endTime - startTime) + " ms");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static void executeWithGinIndex(String query , HashMap<String,String> hashMap, Connection conn) {
        try {
            conn.setAutoCommit(false);
            conn.createStatement().execute("SET enable_seqscan = off");
            for (Map.Entry<String, String> entry : hashMap.entrySet()) {
                conn.createStatement().execute("CREATE INDEX " + entry.getKey() + "_" + entry.getValue() + "_index ON " + entry.getValue() + " USING GIN(" + entry.getKey() + ");");
            }
            long startTime = System.currentTimeMillis();
            conn.createStatement().execute(query);
            long endTime = System.currentTimeMillis();
            for (Map.Entry<String, String> entry : hashMap.entrySet()) {
                conn.createStatement().execute("DROP INDEX " + entry.getKey() + "_" + entry.getValue() + "_index;");
            }
            conn.createStatement().execute("SET enable_seqscan = on");
            conn.commit();
            System.out.println("Time taken to execute query with Gin index is " + (endTime - startTime) + " ms");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static void executeWithBrinIndex(String query , HashMap<String,String> hashMap, Connection conn) {
        try {
            conn.setAutoCommit(false);
            conn.createStatement().execute("SET enable_seqscan = off");
            for (Map.Entry<String, String> entry : hashMap.entrySet()) {
                conn.createStatement().execute("CREATE INDEX " + entry.getKey() + "_" + entry.getValue() + "_index ON " + entry.getValue() + " USING BRIN(" + entry.getKey() + ");");
            }
            long startTime = System.currentTimeMillis();
            conn.createStatement().execute(query);
            long endTime = System.currentTimeMillis();
            for (Map.Entry<String, String> entry : hashMap.entrySet()) {
                conn.createStatement().execute("DROP INDEX " + entry.getKey() + "_" + entry.getValue() + "_index;");
            }
            conn.createStatement().execute("SET enable_seqscan = on");
            conn.commit();
            System.out.println("Time taken to execute query with Brin index is " + (endTime - startTime) + " ms");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }










}
