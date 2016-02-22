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

    public static Map<List<String>, List<String>> check2NF(String tableName, List<String> candidateKey, List<String> nonKeyAttributes) throws SQLException {

        Map<List<String>, List<String>> mapFDs = new HashMap<>();

        if (candidateKey.size() > 1) {
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
                    System.out.println(sqlQuery);
                    ResultSet rs = DbConnection.executeQuery(sqlQuery);
                    int ckCount = 0;

                    if (rs.next()) {
                        System.out.println(rs.getInt(1));
                        ckCount = rs.getInt(1);
                    }

                    for (String nonKey : nonKeyAttributes) {
                        sqlQuery = GenerateSQL.getDistinctCount(tableName, Arrays.asList(candidateKey.get(i), candidateKey.get((i+1) % 3), nonKey));
                        System.out.println(sqlQuery);
                        rs = DbConnection.executeQuery(sqlQuery);
                        int nkCount = 0;

                        if (rs.next()) {
                            System.out.println(rs.getInt(1));
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

    public static Map<List<String>, List<String>> check3NF(String tableName, List<String> nonKeyAttributes) throws SQLException {

        Map<List<String>, List<String>> mapFDs = new HashMap<>();

        if (nonKeyAttributes.size() > 1) {
            // check FDs against individual non key attributes
            for (String nKey1 : nonKeyAttributes) {
                String sqlQuery = GenerateSQL.getDistinctCount(tableName, nKey1);
                System.out.println(sqlQuery);
                ResultSet rs = DbConnection.executeQuery(sqlQuery);
                int count1 = 0;

                if (rs.next()) {
                    System.out.println(rs.getInt(1));
                    count1 = rs.getInt(1);
                }

                for (String nKey2 : nonKeyAttributes) {
                    if (!nKey1.equals(nKey2)) {
                        sqlQuery = GenerateSQL.getDistinctCount(tableName, Arrays.asList(nKey1, nKey2));
                        System.out.println(sqlQuery);
                        rs = DbConnection.executeQuery(sqlQuery);
                        int count2 = 0;

                        if (rs.next()) {
                            System.out.println(rs.getInt(1));
                            count2 = rs.getInt(1);
                        }

                        if (count1 == count2) {
                            if (mapFDs.containsKey(Arrays.asList(nKey1))) {
                                List<String> nonKeys = new ArrayList<>();
                                nonKeys.addAll(mapFDs.get(Arrays.asList(nKey1)));
                                nonKeys.add(nKey2);
                                mapFDs.replace(Arrays.asList(nKey1), nonKeys);
                            } else {
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
    
    // Decomposition
    public static void decompose(String tabName, List<String> candidateKey, List<String> nonKeyAttribute, Map<List<String>, List<String>> partialFD) {
		// Identify each partial FD: Already done in previous step
    	// initialization
    	Set<String> ckSet = new HashSet<String>(candidateKey);
    	Set<String> nckSet = new HashSet<String>(nonKeyAttribute);
    	Set<String> allattributesSet = new HashSet<>();
    	allattributesSet.addAll(ckSet);
    	allattributesSet.addAll(nckSet);
    	
    	System.out.println(allattributesSet);
    	
    	// Split Partial Functional Dependency
    	Map<List<String>, List<String>> splitedPartialFD = partialFD;
    	
    	if (checkclosure(allattributesSet, candidateKey, splitedPartialFD)) {
			
		}
    	// Remove the attributes that depend on each of the determinants so identified
    	// Place these determinants in separate relations along with their dependent attributes
    	// In original relation keep the composite key and any attributes that are fully functionally dependent on all of it
    	// Even if the composite key has no dependent attributes, keep that relation to connect logically the others
	}
    
    /*
    public static Map<List<String>, List<String>> getSplitFD(Map<List<String>, List<String>> partialFD) {
		Map<List<String>, List<String>> newSplittedFD  = new HashMap<List<String>, List<String>>();
		for(Map.Entry<List<String>, List<String>> entry: partialFD.entrySet()){
			// split functional dependency so that FD has a single attribute on the right
			for(String nkey: entry.getValue()){
				List<String> key = entry.getKey();
				//newSplittedFD.put(key, value)
			}
		}
		
		return newSplittedFD;
	}*/
    
    public static boolean checkclosure(Set<String> allattributesSet, List<String> keys, Map<List<String>, List<String>> splitedPartialFD) {
    	Set<String> closure = getClosure(keys, splitedPartialFD);
    	if (!allattributesSet.equals(closure)) {
    		System.err.println("Not sufficient!");
			return false;
		}
		return true;
	}
    
    // get the closure of a set of attributes
    public static Set<String> getClosure(List<String> attribute, Map<List<String>, List<String>> splitedPartialFD) {
		//initialization
    	Set<String> closure = new HashSet<String>(attribute);
    	
    	boolean flag = true;
		while (flag) {
			// get all candidate of closure
			List<ArrayList<String>> subsetList = getSubset(closure); 
		}
		return closure;
	}
    
    // get subset from closure
    public static List<ArrayList<String>> getSubset( Set<String> closure) {
		List<ArrayList<String>> subsets = new ArrayList<ArrayList<String>>();
		ArrayList<String> elementList = new ArrayList<String>(closure);
		System.out.println(elementList);
		subsets.add(new ArrayList<>());
		//subsets.add(elementList);
		
		for (int i = 0; i < elementList.size(); i++) {
			int curSize = subsets.size();
			for (int j = 0; j < curSize; j++) {
				ArrayList<String> curList = new ArrayList<String>(subsets.get(j));
				curList.add(elementList.get(i));
				subsets.add(curList);
			}
		}
		
		subsets.remove(0);
		System.out.println(subsets);
		return subsets;
	}
    
    // verify the decomposition
    public static boolean decompositionVerify() {
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
            Map<List<String>, List<String>> partialFD;
            Map<List<String>, List<String>> transitiveFD;

            try {
                if (checkTableExist(tableName)) {
                    if (checkColumnAndTableExist(tableName, candidateKey)) {
                        if (checkColumnAndTableExist(tableName, nonKey)) {
                            if (!check1NF_nulls(tableName, candidateKey)) {
                                System.err.println("Table " + tableName + " not in 1NF: null keys\n");
                            }
                            else if (!check1NF_duplicates(tableName, candidateKey)) {
                                System.out.println("Table " + tableName + " not in 1NF: duplicate keys\n");
                            }
                            else {
                                partialFD = check2NF(tableName, candidateKey, nonKey);
                                if (!partialFD.isEmpty()) {
                                    System.out.println("Table " + tableName + " not in 2NF\n");
                                    for (Map.Entry<List<String>, List<String>> entry : partialFD.entrySet()) {

                                        for (String key : entry.getKey()) {
                                            System.out.print(key + " ");
                                        }
                                        System.out.print("->");
                                        for (String nKey : entry.getValue()) {
                                            System.out.print(" " + nKey);
                                        }
                                        System.out.println();
                                    }
                                    // decompose the table
                                    decompose(tableName, candidateKey, nonKey, partialFD);
                                }
                                else {
                                    transitiveFD = check3NF(tableName, nonKey);
                                    if (!transitiveFD.isEmpty()) {
                                        System.out.println("Table " + tableName + " not in 3NF\n");
                                        for (Map.Entry<List<String>, List<String>> entry : transitiveFD.entrySet()) {

                                            for (String key : entry.getKey()) {
                                                System.out.print(key + " ");
                                            }
                                            System.out.print("->");
                                            for (String nKey : entry.getValue()) {
                                                System.out.print(" " + nKey);
                                            }
                                            System.out.println();
                                        }
                                    }
                                    else {
                                        System.out.println("Table " + tableName + " is in 3NF\n");
                                    }
                                }
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
