package nf;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class CertifyNF {

    // Check the table name exist in the database
    public static boolean checkTableExist(String tableName) throws SQLException {

        String sqlQuery = GenerateSQL.getTableExist(tableName);
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

    // Check if the candidate key has null value
    public static boolean check1NF_nulls(String tableName, List<String> candidateKey) throws SQLException {

        String sqlQuery = GenerateSQL.checkNulls(tableName, candidateKey);
        ResultSet rs = DbConnection.executeQuery(sqlQuery);

        if (rs.next()) {
            if (rs.getInt(1) == 0)
                return true;
        }

        return false;
    }

    // Check if the candidate key has duplicate value
    public static boolean check1NF_duplicates(String tableName, List<String> candidateKey) throws SQLException {

        String sqlQuery = GenerateSQL.getTotalCount(tableName);
        ResultSet rs = DbConnection.executeQuery(sqlQuery);
        int totalCount = 0;

        if (rs.next()) {
            totalCount = rs.getInt(1);
        }

        sqlQuery = GenerateSQL.getDistinctCount(tableName, candidateKey);
        rs = DbConnection.executeQuery(sqlQuery);
        int ckCount = 0;

        if (rs.next()) {
            ckCount = rs.getInt(1);
        }

        return totalCount == ckCount;
    }

    // Check if the table is in 2NF, try to find partial functional dependencies
    public static Map<List<String>, List<String>> check2NF(String tableName, List<String> candidateKey, List<String> nonKeyAttributes) throws SQLException {

        Map<List<String>, List<String>> mapFDs = new LinkedHashMap<>();

        if (candidateKey.size() > 1) {
            // check FD's against individual candidate key
            for (String cK : candidateKey) {
                String sqlQuery = GenerateSQL.getDistinctCount(tableName, cK);
                ResultSet rs = DbConnection.executeQuery(sqlQuery);
                int ckCount = 0;

                if (rs.next()) {
                    ckCount = rs.getInt(1);
                }

                for (String nonKey : nonKeyAttributes) {
                    sqlQuery = GenerateSQL.getDistinctCount(tableName, Arrays.asList(cK, nonKey));
                    rs = DbConnection.executeQuery(sqlQuery);
                    int nkCount = 0;

                    if (rs.next()) {
                        nkCount = rs.getInt(1);
                    }

                    if (ckCount == nkCount) {
                        if (mapFDs.containsKey(Arrays.asList(cK))){
                            List<String> nonKeys = new ArrayList<>();
                            nonKeys.addAll(mapFDs.get(Arrays.asList(cK)));
                            nonKeys.add(nonKey);
                            mapFDs.replace(Arrays.asList(cK), nonKeys);
                        }
                        else {
                            mapFDs.put(Arrays.asList(cK), Arrays.asList(nonKey));
                        }
                    }
                }
            }

            // check against subset of candidate keys
            if (candidateKey.size() > 2) {
                for (int i = 0; i < candidateKey.size(); i++) {
                    List<String> subsetCK = Arrays.asList(candidateKey.get(i), candidateKey.get((i+1) % 3));
                    String sqlQuery = GenerateSQL.getDistinctCount(tableName, subsetCK);
                    ResultSet rs = DbConnection.executeQuery(sqlQuery);
                    int ckCount = 0;

                    if (rs.next()) {
                        ckCount = rs.getInt(1);
                    }

                    for (String nonKey : nonKeyAttributes) {
                        sqlQuery = GenerateSQL.getDistinctCount(tableName, Arrays.asList(candidateKey.get(i), candidateKey.get((i+1) % 3), nonKey));
                        rs = DbConnection.executeQuery(sqlQuery);
                        int nkCount = 0;

                        if (rs.next()) {
                            nkCount = rs.getInt(1);
                        }

                        if (ckCount == nkCount) {
                            if (mapFDs.containsKey(subsetCK)){
                                List<String> nonKeys = mapFDs.get(subsetCK);
                                nonKeys.add(nonKey);
                                mapFDs.replace(subsetCK, nonKeys);
                            }
                            else {
                                mapFDs.put(subsetCK, Arrays.asList(nonKey));
                            }
                        }
                    }
                }
            }
        }
        return mapFDs;
    }

    // Check if the table is in 3NF, try to find transitive functional dependencies
    public static Map<List<String>, List<String>> check3NF(String tableName, List<String> nonKeyAttributes) throws SQLException {

        Map<List<String>, List<String>> mapFDs = new HashMap<>();

        if (nonKeyAttributes.size() > 1) {
            // check FDs against individual non key attributes
            for (String nKey1 : nonKeyAttributes) {
                String sqlQuery = GenerateSQL.getDistinctCount(tableName, nKey1);
                ResultSet rs = DbConnection.executeQuery(sqlQuery);
                int count1 = 0;

                if (rs.next()) {
                    count1 = rs.getInt(1);
                }

                for (String nKey2 : nonKeyAttributes) {
                    if (!nKey1.equals(nKey2)) {
                        sqlQuery = GenerateSQL.getDistinctCount(tableName, Arrays.asList(nKey1, nKey2));
                        rs = DbConnection.executeQuery(sqlQuery);
                        int count2 = 0;

                        if (rs.next()) {
                            count2 = rs.getInt(1);
                        }

                        if (count1 == count2) {
                            if (mapFDs.containsKey(Arrays.asList(nKey1))) {
                                List<String> nonKeys = new ArrayList<>();
                                nonKeys.addAll(mapFDs.get(Arrays.asList(nKey1)));
                                nonKeys.add(nKey2);
                                mapFDs.replace(Arrays.asList(nKey1), nonKeys);
                            }
                            else {
                                mapFDs.put(Arrays.asList(nKey1), Arrays.asList(nKey2));
                            }
                        }
                    }
                }
            }

            // check FDs against subsets of non key attributes
            if (nonKeyAttributes.size() > 2) {
                for (int i = 0; i < nonKeyAttributes.size(); i++) {
                    for (int j = i+1; j < nonKeyAttributes.size(); j++) {
                        List<String> subsetNonKeys = Arrays.asList(nonKeyAttributes.get(i), nonKeyAttributes.get(j));
                        String sqlQuery = GenerateSQL.getDistinctCount(tableName, subsetNonKeys);
                        ResultSet rs = DbConnection.executeQuery(sqlQuery);
                        int count1 = 0;

                        if (rs.next()) {
                            count1 = rs.getInt(1);
                        }

                        for (String nKey : nonKeyAttributes) {
                            if (!nKey.equals(nonKeyAttributes.get(i)) && !nKey.equals(nonKeyAttributes.get(j))) {
                                sqlQuery = GenerateSQL.getDistinctCount(tableName, Arrays.asList(nonKeyAttributes.get(i), nonKeyAttributes.get(j), nKey));
                                rs = DbConnection.executeQuery(sqlQuery);
                                int count2 = 0;

                                if (rs.next()) {
                                    count2 = rs.getInt(1);
                                }

                                if (count1 == count2) {
                                    if (mapFDs.containsKey(subsetNonKeys)){
                                        List<String> nonKeys = mapFDs.get(subsetNonKeys);
                                        nonKeys.add(nKey);
                                        mapFDs.replace(subsetNonKeys, nonKeys);
                                    }
                                    else {
                                        mapFDs.put(subsetNonKeys, Arrays.asList(nKey));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return mapFDs;

    }

    // Decompose the tables recursively
    public static Map<List<String>, List<String>> decompose(String tabName, List<String> candidateKey, List<String> nonKeyAttribute,
                                                            Map<List<String>, List<String>> partialFD) throws SQLException {

        List<String> ck1 = candidateKey;
        List<String> nck1 = nonKeyAttribute;

        // get the first key in the hash map
        List<String> ck2 = partialFD.keySet().iterator().next();
        // get the value from the hash map
        List<String> nck2 = partialFD.get(ck2);

        nck1.removeAll(nck2);

        Map<List<String>, List<String>> pFD1 = check2NF(tabName, ck1, nck1);
        Map<List<String>, List<String>> pFD2 = check2NF(tabName, ck2, nck2);

        Map<List<String>, List<String>> T = new LinkedHashMap<List<String>, List<String>>();
        Map<List<String>, List<String>> T1 = new LinkedHashMap<List<String>, List<String>>();
        Map<List<String>, List<String>> T2 = new LinkedHashMap<List<String>, List<String>>();

        if (!pFD1.isEmpty()) {
            T1 = decompose(tabName, ck1, nck1, pFD1);
            T.putAll(T1);
        }
        else {
            T1.put(ck1, nck1);
            T.putAll(T1);
        }

        if (!pFD2.isEmpty()) {
            T2 = decompose(tabName, ck2, nck2, pFD2);
            T.putAll(T2);
        }
        else {
            T2.put(ck2, nck2);
            T.putAll(T2);
        }

        return T;
    }

    // verify the decomposition
    public static boolean decompositionVerify(Map<List<String>, List<String>> relations, String tableName) throws SQLException {

        int n = 1;
        List<String> decomposedTables = new ArrayList<>();
        List<String> mainTable = new ArrayList<>();
        boolean first = true;

        // Create temp tables in database for each decomposed table
        for (Map.Entry<List<String>,List<String>> entry : relations.entrySet()) {
            List<String> allAttributes = new ArrayList<>();
            allAttributes.addAll(entry.getKey());
            allAttributes.addAll(entry.getValue());

            if (first) {
                mainTable.addAll(allAttributes);
                first = false;
            }

            String tabName = tableName + "_" + n++;
            decomposedTables.add(tabName);
            String sqlQuery = GenerateSQL.createTempTable(tableName, tabName, allAttributes);
            DbConnection.execute(sqlQuery);
        }

        // Get the keys for the decomposed tables
        List<List<String>> foreignKeys = new ArrayList<>();
        List<List<String>> allKeys = new ArrayList<>();
        allKeys.addAll(relations.keySet());

        for (int i = 1; i < allKeys.size(); i++) {
            List<String> list1 = mainTable;
            List<String> list2 = allKeys.get(i);
            List<String> list3 = list1.stream().filter(list2::contains).collect(Collectors.toList());
            foreignKeys.add(list3);
        }

        // Join the decomposed tables and verify the count with the original table
        String sqlQuery = GenerateSQL.getCountJoinTables(decomposedTables, foreignKeys);
        ResultSet rs = DbConnection.executeQuery(sqlQuery);
        int count1 = 0;
        if (rs.next()) {
            count1 = rs.getInt(1);
        }

        sqlQuery = GenerateSQL.getTotalCount(tableName);
        rs = DbConnection.executeQuery(sqlQuery);
        int count2 = 0;
        if (rs.next()) {
            count2 = rs.getInt(1);
        }

        return count1 == count2;
    }

    // print decomposition
    public static void printDecomposition(List<Map<List<String>, List<String>>> decompositionList,
                                          List<String> decomposedTableNameList, List<Boolean> decompositionVerification) throws IOException {

        for (int iter = 0; iter < decomposedTableNameList.size(); iter++) {
            Output.writeResult("\n" + decomposedTableNameList.get(iter) + " decomposition:\n");
            Map<List<String>, List<String>> relation = decompositionList.get(iter);
            int j = 0;
            StringBuilder stringBuilder = new StringBuilder();
            for (Map.Entry<List<String>, List<String>> entry : relation.entrySet()) {
                j++;
                Output.writeResult(" " + decomposedTableNameList.get(iter) + "_" + j + "(");
                stringBuilder.append(decomposedTableNameList.get(iter) + "_" + j + ",");

                List<String> ck = entry.getKey();
                List<String> nk = entry.getValue();

                for (int i = 0; i < ck.size(); i++) {
                    if (i == ck.size() - 1) {
                        if (nk.size() == 0) {
                            Output.writeResult(ck.get(i));
                        }
                        else {
                            Output.writeResult(ck.get(i) + ",");
                        }
                    }
                    else {
                        Output.writeResult(ck.get(i) + ",");
                    }
                }

                for (int i = 0; i < nk.size(); i++) {
                    if (i == nk.size() - 1) {
                        Output.writeResult(nk.get(i));
                    }
                    else {
                        Output.writeResult(nk.get(i) + ",");
                    }
                }
                Output.writeResult(")\n");
            }

            String verifyFlag = "NO";
            if (decompositionVerification.get(iter)) {
                verifyFlag = "YES";
            }

            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            Output.writeResult("Verification:\n");
            Output.writeResult(" " + decomposedTableNameList.get(iter) + " = join(" + stringBuilder.toString() + ")? " + verifyFlag + "\n");

        }
    }

    public static void main(String[] args) throws IOException{

        // Parse input argument
        if(!ArgParser.parse(args))
            return;

        // Read input file and extract table and columns details
        ArgParser.readFile();
        List<String> tableNames = ArgParser.tableNames;
        List<List<String>> candidateKeys = ArgParser.candidateKeys;
        List<List<String>> nonKeyAttributes = ArgParser.nonKeyAttributes;

        // Connect to database
        if (!DbConnection.connect())
            return;

        // Generate the result file
        Output.createFile();
        GenerateSQL.createFile();

        List<Map<List<String>, List<String>>> decompositionList = new ArrayList<>();
        List<String> decomposedTableNameList = new ArrayList<>();
        List<Boolean> decompositionVerification = new ArrayList<>();

        for (int i = 0; i < tableNames.size(); i++) {
            String tableName = tableNames.get(i);
            List<String> candidateKey = candidateKeys.get(i);
            List<String> nonKey = nonKeyAttributes.get(i);
            // sort it alphabetically
            java.util.Collections.sort(candidateKey);
            java.util.Collections.sort(nonKey);

            Map<List<String>, List<String>> partialFD;
            Map<List<String>, List<String>> transitiveFD;
            Map<List<String>, List<String>> relations;
            Boolean flag3NF = false;
            String explanation = "";

            Output.writeResult(tableName + "\t 3NF");
            System.out.println("\nChecking the table: " + tableName);

            try {
                if (!checkTableExist(tableName)) {
                    explanation = "Invalid table";
                }
                else if (!checkColumnAndTableExist(tableName, candidateKey) || !checkColumnAndTableExist(tableName, nonKey)) {
                    explanation = "Invalid column";
                }
                else if (!check1NF_nulls(tableName, candidateKey)) {
                    explanation = "not 1NF, null keys";
                }
                else if (!check1NF_duplicates(tableName, candidateKey)) {
                    explanation = "not 1NF, duplicate keys";
                }
                else {
                    partialFD = check2NF(tableName, candidateKey, nonKey);
                    if (!partialFD.isEmpty()) {
                        explanation = "not 2NF, ";

                        for (Map.Entry<List<String>, List<String>> entry : partialFD.entrySet()) {
                            for (String key : entry.getKey()) {
                                explanation += key;
                            }
                            explanation += " -> ";

                            for (String nKey : entry.getValue()) {
                                explanation += nKey;
                            }
                            explanation += ", ";
                        }
                        explanation = explanation.substring(0, explanation.length() - 2);

                        // decompose the table
                        relations = decompose(tableName, candidateKey, nonKey, partialFD);
                        Boolean flag = decompositionVerify(relations, tableName);
                        decompositionList.add(relations);
                        decomposedTableNameList.add(tableName);
                        decompositionVerification.add(flag);
                    }
                    else {
                        transitiveFD = check3NF(tableName, nonKey);
                        if (!transitiveFD.isEmpty()) {
                            explanation = "not 3NF, ";

                            for (Map.Entry<List<String>, List<String>> entry : transitiveFD.entrySet()) {
                                for (String key : entry.getKey()) {
                                    explanation += key;
                                }
                                explanation += " -> ";
                                for (String nKey : entry.getValue()) {
                                    explanation += nKey;
                                }
                                explanation +=", ";
                            }
                            explanation = explanation.substring(0, explanation.length() - 2);

                            relations = decompose(tableName, candidateKey, nonKey, transitiveFD);
                            Boolean flag = decompositionVerify(relations, tableName);
                            decompositionList.add(relations);
                            decomposedTableNameList.add(tableName);
                            decompositionVerification.add(flag);
                        }
                        else {
                            flag3NF = true;
                        }
                    }
                }
            }
            catch (SQLException e) {
                System.err.println("Could not execute query");
                e.printStackTrace();
            }

            if (flag3NF == true) {
                Output.writeResult("\tY\n");
                System.out.println("Table " + tableName + " is in 3NF");
            }
            else {
                Output.writeResult("\tN\t" + explanation + "\n");
                System.out.println(explanation);
            }

        }

        DbConnection.closeConnection();

        // print the decomposition
        printDecomposition(decompositionList, decomposedTableNameList, decompositionVerification);

        System.out.println("Done");
    }
}
