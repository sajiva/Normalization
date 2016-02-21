package nf;

import java.sql.*;
import java.util.*;

public class CertifyNF {

    public static boolean check1NF_nulls(String tableName, List<String> candidateKey) throws SQLException {

        String sqlQuery = GenerateSQL.checkNulls(tableName, candidateKey);
        System.out.println(sqlQuery);
        ResultSet rs = DbConnection.executeQuery(sqlQuery);

        if (rs.next()) {
            System.out.println(rs.getInt(1));
            if (rs.getInt(1) == 0)
                return true;
        }

        return false;
    }

    public static boolean check1NF_duplicates(String tableName, List<String> candidateKey) throws SQLException {

        String sqlQuery = GenerateSQL.getTotalCount(tableName);
        System.out.println(sqlQuery);
        ResultSet rs = DbConnection.executeQuery(sqlQuery);
        int totalCount = 0;

        if (rs.next()) {
            System.out.println(rs.getInt(1));
            totalCount = rs.getInt(1);
        }

        sqlQuery = GenerateSQL.getDistinctCount(tableName, candidateKey);
        System.out.println(sqlQuery);
        rs = DbConnection.executeQuery(sqlQuery);
        int ckCount = 0;

        if (rs.next()) {
            System.out.println(rs.getInt(1));
            ckCount = rs.getInt(1);
        }

        return totalCount == ckCount;
    }

    public static boolean check2NF(String tableName, List<String> candidateKey, List<String> nonKeyAttributes) throws SQLException {

        if (candidateKey.size() == 1)
            return true;
        else {
            // check FD's against individual candidate key
            for (String cK : candidateKey) {
                String sqlQuery = GenerateSQL.getDistinctCount(tableName, cK);
                System.out.println(sqlQuery);
                ResultSet rs = DbConnection.executeQuery(sqlQuery);
                int ckCount = 0;

                if (rs.next()) {
                    System.out.println(rs.getInt(1));
                    ckCount = rs.getInt(1);
                }

                for (String nonKey : nonKeyAttributes) {
                    sqlQuery = GenerateSQL.getDistinctCount(tableName, Arrays.asList(cK, nonKey));
                    System.out.println(sqlQuery);
                    rs = DbConnection.executeQuery(sqlQuery);
                    int nkCount = 0;

                    if (rs.next()) {
                        System.out.println(rs.getInt(1));
                        nkCount = rs.getInt(1);
                    }

                    if (ckCount == nkCount)
                        return false;
                }
            }

            // check against subset of candidate keys
            if (candidateKey.size() > 2) {
                for (int i = 0; i < candidateKey.size(); i++) {
                    String sqlQuery = GenerateSQL.getDistinctCount(tableName, Arrays.asList(candidateKey.get(i), candidateKey.get((i+1) % 3)));
                    System.out.println(sqlQuery);
                    ResultSet rs = DbConnection.executeQuery(sqlQuery);
                    int ckCount = 0;

                    if (rs.next()) {
                        System.out.println(rs.getInt(1));
                        ckCount = rs.getInt(1);
                    }

                    for (String nonKey : nonKeyAttributes) {
                        sqlQuery = GenerateSQL.getDistinctCount(tableName, Arrays.asList(candidateKey.get(i), candidateKey.get(i+1), nonKey));
                        System.out.println(sqlQuery);
                        rs = DbConnection.executeQuery(sqlQuery);
                        int nkCount = 0;

                        if (rs.next()) {
                            System.out.println(rs.getInt(1));
                            nkCount = rs.getInt(1);
                        }

                        if (ckCount == nkCount)
                            return false;
                    }
                }
            }
        }
        return true;
    }

    public static boolean check3NF(String tableName, List<String> nonKeyAttributes) throws SQLException {

        if (nonKeyAttributes.size() == 1)
            return true;
        else {
            // check FDs against individual non key attributes
            for (String nkey1 : nonKeyAttributes) {
                String sqlQuery = GenerateSQL.getDistinctCount(tableName, nkey1);
                System.out.println(sqlQuery);
                ResultSet rs = DbConnection.executeQuery(sqlQuery);
                int count1 = 0;

                if (rs.next()) {
                    System.out.println(rs.getInt(1));
                    count1 = rs.getInt(1);
                }

                for (String nKey2 : nonKeyAttributes) {
                    if (!nkey1.equals(nKey2)) {
                        sqlQuery = GenerateSQL.getDistinctCount(tableName, Arrays.asList(nkey1, nKey2));
                        System.out.println(sqlQuery);
                        rs = DbConnection.executeQuery(sqlQuery);
                        int count2 = 0;

                        if (rs.next()) {
                            System.out.println(rs.getInt(1));
                            count2 = rs.getInt(1);
                        }

                        if (count1 == count2)
                            return false;
                    }
                }
            }

            // check FDs against subsets of non key attributes
            if (nonKeyAttributes.size() > 2) {
                for (int i = 0; i < nonKeyAttributes.size(); i++) {
                    for (int j = i+1; j < nonKeyAttributes.size(); j++) {
                        String sqlQuery = GenerateSQL.getDistinctCount(tableName, Arrays.asList(nonKeyAttributes.get(i), nonKeyAttributes.get(j)));
                        System.out.println(sqlQuery);
                        ResultSet rs = DbConnection.executeQuery(sqlQuery);
                        int count1 = 0;

                        if (rs.next()) {
                            System.out.println(rs.getInt(1));
                            count1 = rs.getInt(1);
                        }

                        for (String nKey : nonKeyAttributes) {
                            if (!nKey.equals(nonKeyAttributes.get(i)) && !nKey.equals(nonKeyAttributes.get(j))) {
                                sqlQuery = GenerateSQL.getDistinctCount(tableName, Arrays.asList(nonKeyAttributes.get(i), nonKeyAttributes.get(j), nKey));
                                System.out.println(sqlQuery);
                                rs = DbConnection.executeQuery(sqlQuery);
                                int count2 = 0;

                                if (rs.next()) {
                                    System.out.println(rs.getInt(1));
                                    count2 = rs.getInt(1);
                                }

                                if (count1 == count2)
                                    return false;
                            }
                        }
                    }
                }
            }
        }

        return true;

    }
    
    // Check the table name exist in the database
    public static boolean checkTableExist(String tableName) throws SQLException {

        String sqlQuery = GenerateSQL.getTableExist(tableName);
    	System.out.println(sqlQuery);
    	ResultSet rs = DbConnection.executeQuery(sqlQuery);

        if (rs.next()) {
            if (0 ==rs.getInt(1)) {
                System.err.println("Cannot find the table " + tableName);
                return false;
            }
        }

		return true;
	}
    
    // Check Column exist
    public static boolean checkColumnAndTableExist(String tableName, List<String> columnNames) throws SQLException {

        for (String columnName : columnNames) {
            String sqlQuery = GenerateSQL.getColumnAndTableExist(tableName, columnName);
            System.out.println(sqlQuery);
            ResultSet rs = DbConnection.executeQuery(sqlQuery);

            if (rs.next()) {
                if (0 == rs.getInt(1)) {
                    System.err.println("Cannot find the column " + columnName + " in table " + tableName);
                    return false;
                }
            }
        }

		return true;
	}
    
    
    public static void main(String[] args) {

        // Parse input argument
        if(!ArgParser.parse(args))
            return;

        // Read input file and extract table and columns details
        ArgParser.readFile();
        List<String> tableNames = ArgParser.tableNames;
        List<List<String>> candidateKeys = ArgParser.candidateKeys;
        List<List<String>> nonKeyAttributes = ArgParser.nonKeyAttributes;

    	//Check database connection
        if (!DbConnection.connect())
            return;


        for (int i = 0; i < tableNames.size(); i++) {
            String tableName = tableNames.get(i);
            List<String> candidateKey = candidateKeys.get(i);
            List<String> nonKey = nonKeyAttributes.get(i);

            try {
                if (checkTableExist(tableName)) {
                    if (checkColumnAndTableExist(tableName, candidateKey)) {
                        if (checkColumnAndTableExist(tableName, nonKey)) {
                            if (!check1NF_nulls(tableName, candidateKey)) {
                                System.err.println("Table " + tableName + " not in 1NF: null keys\n");
                            } else if (!check1NF_duplicates(tableName, candidateKey)) {
                                System.out.println("Table " + tableName + " not in 1NF: duplicate keys\n");
                            } else if (!check2NF(tableName, candidateKey, nonKey)) {
                                System.out.println("Table " + tableName + " not in 2NF\n");
                            } else if (!check3NF(tableName, nonKey)) {
                                System.out.println("Table " + tableName + " not in 3NF\n");
                            } else {
                                System.out.println("Table " + tableName + " is in 3NF\n");
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                System.err.println("Could not execute query");
                e.printStackTrace();
            }
        }

        DbConnection.closeConnection();

    }
}
