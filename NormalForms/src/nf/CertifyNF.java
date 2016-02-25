package nf;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class CertifyNF {

    public static boolean check1NF_nulls(String tableName, List<String> candidateKey) throws SQLException {

        String sqlQuery = GenerateSQL.checkNulls(tableName, candidateKey);
        ResultSet rs = DbConnection.executeQuery(sqlQuery);

        if (rs.next()) {
            if (rs.getInt(1) == 0)
                return true;
        }

        return false;
    }

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

    public static Map<List<String>, List<String>> check3NF(String tableName, List<String> nonKeyAttributes) throws SQLException {

        Map<List<String>, List<String>> mapFDs = new LinkedHashMap<>();

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
    
    // Check the table name exist in the database
    public static boolean checkTableExist(String tableName) throws SQLException {

        String sqlQuery = GenerateSQL.getTableExist(tableName);
    	//System.out.println(sqlQuery);
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
            //System.out.println(sqlQuery);
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
    public static Map<List<String>, List<String>> decomposeTo2NF(String tabName, List<String> candidateKey, List<String> nonKeyAttribute, 
    		Map<List<String>, List<String>> partialFD) throws SQLException {
		// Identify each partial FD: Already done in previous step
    	// initialization
    	//Set<String> ckSet = new HashSet<String>(candidateKey);
    	//Set<String> nckSet = new HashSet<String>(nonKeyAttribute);
    	//Set<String> allattributesSet = new HashSet<>();
    	//allattributesSet.addAll(ckSet);
    	//allattributesSet.addAll(nckSet);
    	
    	//System.out.println(allattributesSet);
    	
    	// Split Partial Functional Dependency
    	//Map<List<String>, List<String>> splitedPartialFD = partialFD;
    	Map<List<String>, List<String>> relations = splitRelation(tabName, candidateKey, nonKeyAttribute, partialFD);
    	/*
    	if (checkclosure(allattributesSet, candidateKey, splitedPartialFD)) {
			splitRelation(tabName, candidateKey, nonKeyAttribute, partialFD);
		}*/
    	return relations;
	}
    
    // recursively decompose schema
    public static Map<List<String>, List<String>> splitRelation(String tabName, List<String> candidateKey, List<String> nonKeyAttribute, 
    		Map<List<String>, List<String>> partialFD) throws SQLException {
    	// get the set
    	//Set<String> ckSet = new HashSet<String>(candidateKey);
    	//Set<String> nckSet = new HashSet<String>(nonKeyAttribute);
    	//Set<String> allSet = new HashSet<>();
    	//allSet.addAll(ckSet);
    	//allSet.addAll(nckSet);
		// choose one functional dependency.
    	
    	//// get the first key in the hash map

        List<String> ck1 = candidateKey;
        List<String> nck1 = nonKeyAttribute;

    	List<String> ck2 = partialFD.keySet().iterator().next();
    	// do not need to get closure
    	//Set<String> R1 = getClosure(ck1, partialFD);
    	//// get the value from the hash map
    	List<String> nck2 = partialFD.get(ck2);
    	/// get the new partial functional dependency
    	
//    	List<String> ck2 = candidateKey;
//    	List<String> nck2 = nonKeyAttribute;
    	nck1.removeAll(nck2);
    	
    	Map<List<String>, List<String>> pFD1 = check2NF(tabName, ck1, nck1);
    	
    	//System.out.println(Arrays.toString(pFD1.entrySet().toArray()));
    	
    	Map<List<String>, List<String>> pFD2 = check2NF(tabName, ck2, nck2);
    	
    	//System.out.println(Arrays.toString(pFD2.entrySet().toArray()));
    	
    	Map<List<String>, List<String>> T = new LinkedHashMap<List<String>, List<String>>();
    	Map<List<String>, List<String>> T1 = new LinkedHashMap<List<String>, List<String>>();
    	Map<List<String>, List<String>> T2 = new LinkedHashMap<List<String>, List<String>>();
    	
    	if (!pFD1.isEmpty()) {
			T1 = splitRelation(tabName, ck1, nck1, pFD1);
			T.putAll(T1);
		}else {
			T1.put(ck1, nck1);
			T.putAll(T1);
		}
    	
    	if (!pFD2.isEmpty()) {
    		T2 = splitRelation(tabName, ck2, nck2, pFD2);
    		T.putAll(T2);
		}else{
			T2.put(ck2, nck2);
			T.putAll(T2);
		}
    	//System.out.println("The map is: ");
    	//System.out.println(Arrays.toString(T.entrySet().toArray()));
    	
    	return T; 
	}

    /*
    public static List<String> getCandidateKeyfromSet(Set<String> S, List<String> candidateKeys) {
		List<String> ck = new ArrayList<String>();
		for(String attribute: S){
			if (candidateKeys.contains(attribute)) {
				ck.add(attribute);
			}
		}
		return ck;
	}
    
    public static List<String> getNonKeyAttributefromSet(Set<String> S, List<String> nonKeyAttribute) {
		List<String> nk = new ArrayList<String>();
		for(String attribute: S){
			if (nonKeyAttribute.contains(attribute)) {
				nk.add(attribute);
			}
		}
		return nk;
	}*/
    
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
    /*
    public static boolean checkclosure(Set<String> allattributesSet, List<String> keys, Map<List<String>, List<String>> splitedPartialFD) {
    	Set<String> closure = getClosure(keys, splitedPartialFD);
    	if (!allattributesSet.equals(closure)) {
    		System.err.println("Not sufficient!");
			return false;
		}else {
			System.out.println("Sufficient!");
		}
		return true;
	}*/
    
    // get the difference between set1 and set2
    
//    public static Set<String> setDifference(Set<String> set1, Set<String> set2) {
//		set1.removeAll(set2);
//		return set1;
//	}
    
    // get the closure of a set of attributes
//    public static Set<String> getClosure(List<String> attribute, Map<List<String>, List<String>> FD) {
//		//initialization
//    	Set<String> closure = new HashSet<String>(attribute);
//
//    	boolean flag = true;
//		while (flag) {
//			Set<String> tempClosure = new HashSet<String>(closure);
//			// get all candidate of closure
//			List<ArrayList<String>> subsetList = getSubset(closure);
//			// add new element into the set
//			for (ArrayList<String> subset: subsetList) {
//				List<String> element = FD.get(subset);
//				if (element != null) {
//					closure.addAll(element);
//					//System.out.println(closure);
//				}
//			}
//			//System.out.println(closure);
//			//System.out.println(tempClosure);
//			if (tempClosure.containsAll(closure) && closure.containsAll(tempClosure)) {
//				flag = false;
//			}
//		}
//		return closure;
//	}
//
    // get subset from closure
//    public static List<ArrayList<String>> getSubset( Set<String> closure) {
//		List<ArrayList<String>> subsets = new ArrayList<ArrayList<String>>();
//		ArrayList<String> elementList = new ArrayList<String>(closure);
//		//System.out.println(elementList);
//		subsets.add(new ArrayList<>());
//		//subsets.add(elementList);
//
//		for (int i = 0; i < elementList.size(); i++) {
//			int curSize = subsets.size();
//			for (int j = 0; j < curSize; j++) {
//				ArrayList<String> curList = new ArrayList<String>(subsets.get(j));
//				curList.add(elementList.get(i));
//				subsets.add(curList);
//			}
//		}
//
//		subsets.remove(0);
//		//System.out.println(subsets);
//		return subsets;
//	}
//
    
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
            //System.out.println(sqlQuery);
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

        // Join the decomposed tables and verify the cout with the original table
        String sqlQuery = GenerateSQL.getCountJoinTables(decomposedTables, foreignKeys);
        //System.out.println(sqlQuery);
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
    	
    	for(int iter = 0; iter <decomposedTableNameList.size(); iter++){
        	Output.writeResult("\n" + decomposedTableNameList.get(iter) + " decomposition:\n");
        	Map<List<String>, List<String>> relation = decompositionList.get(iter);
        	int j = 0;
        	StringBuilder stringBuilder = new StringBuilder();
    		for(Entry<List<String>, List<String>> entry : relation.entrySet()) {  
                //System.out.println(entry.getKey()+"："+entry.getValue());
    			j++;
    			Output.writeResult(" "+decomposedTableNameList.get(iter)+ "_" + j+"(");
    			
    			stringBuilder.append(decomposedTableNameList.get(iter)+ "_" + j + ",");
    			
    			List<String> ck = entry.getKey();
    			List<String> nk = entry.getValue();
    			
    			for (int i = 0; i < ck.size(); i++) {
    				if (i == ck.size() - 1) {
						if (nk.size() == 0) {
							Output.writeResult(ck.get(i));
						}else {
							Output.writeResult(ck.get(i) + ",");
						}
					}else {
						Output.writeResult(ck.get(i)+",");
					}
				}
    			
    			for (int i = 0; i < nk.size(); i++) {
    				if (i==nk.size()-1) {
						Output.writeResult(nk.get(i));
					}else{
						Output.writeResult(nk.get(i)+",");
					}
				}
    			Output.writeResult(")\n");
            } 
    		String verifyFlag = "NO";
    		if (decompositionVerification.get(iter)) {
				verifyFlag = "YES";
			}
    		
    		stringBuilder.deleteCharAt(stringBuilder.length()-1);
    		Output.writeResult("Verification:\n");
    		Output.writeResult(" "+decomposedTableNameList.get(iter) + " = join(" + stringBuilder.toString()+")? " + verifyFlag + "\n");
    		
        }
	}
    
    // decompose the table into 3NF.
//    public static Map<List<String>, List<String>> decomposeTo3NF(String tabName, List<String> candidateKey,
//    		List<String> nonkeyAttributes, Map<List<String>, List<String>> transitiveFD) {
//		//List<Set<String>> schemaList = new ArrayList<>();
//    	Map<List<String>, List<String>> schemaMap = new HashMap<>();
//		// get all functional dependency
//		Map<List<String>, List<String>> FD = new LinkedHashMap<>();
//		//Because of 2nf, candidate key -> nonKey attributes
//		FD.put(candidateKey, nonkeyAttributes);
//		FD.putAll(transitiveFD);
//		// get the minimal basis of FD
//		Map<List<String>, List<String>> miniCover = getMinimalBasis(candidateKey, FD);
//		// for each functional dependency, get the schema
//		boolean flag = false;
//
//		//************Assume candidate key is a superkey****************//
//		Set<String> ckSet = new HashSet<String>(candidateKey);
//
//		List<Set<String>> currentSetList = new ArrayList<>();
//
//		for(Entry<List<String>, List<String>> entry: miniCover.entrySet()){
//			Set<String> set = new HashSet<String>(entry.getKey());
//			set.addAll(entry.getValue());
//
//			if (currentSetList.size()==0) {
//				currentSetList.add(set);
//				schemaMap.put(entry.getKey(), entry.getValue());
//			}else {
//				boolean addFlag = true;
//				for (int i = 0; i < currentSetList.size(); i++) {
//					Set<String> currentSet = currentSetList.get(i);
//					if (currentSet.containsAll(set)) {
//						addFlag = false;
//					}
//				}
//				if (addFlag) {
//					currentSetList.add(set);
//					//System.out.println(schemaMap);
//					schemaMap.put(entry.getKey(), entry.getValue());
//					//System.out.println(schemaMap);
//				}
//			}
//
//
//			if (set.containsAll(ckSet)) {
//				flag = true;
//			}
//			//schemaList.add(set);
//		}
		
		//schemaMap = miniCover;
		// if no R is a superkey, add schema R0 where R0 is a key of R
//		if (!flag) {
//			schemaMap.put(candidateKey, new ArrayList<>());
//		}
//		return schemaMap;
//	}
    
    
//    public static Map<List<String>, List<String>> getMinimalBasis(List<String> candidateKeys,
//    		Map<List<String>, List<String>> FD) {
//		Map<List<String>, List<String>> basisSchema = new LinkedHashMap<>();
//
//		List<List<String>> left = new ArrayList<>();
//		List<String> right = new ArrayList<>();
//
//		// split the right schema
//		for(Entry<List<String>, List<String>> entry: FD.entrySet()){
//			List<String> rightHandSchema = entry.getValue();
//			List<String> leftHandSchema = entry.getKey();
//			for(int i = 0; i < rightHandSchema.size(); i++){
//				left.add(leftHandSchema);
//				right.add(rightHandSchema.get(i));
//			}
//		}
//
//		// eliminate redundant attributes from LHS
//		List<List<String>> leftnew = new ArrayList<>();
//		List<String> rightnew = right;
//
//		for (int i = 0; i < left.size(); i++) {
//			List<String> lhs = left.get(i);
//			if (lhs.size()>1) {
//				List<Set<String>> closureList = new ArrayList<>();
//				// get the closure for every subset XB -> A
//				String tmpValue = right.get(i);
//
//				for (int j = 0; j < lhs.size(); j++) {
//					List<String> temp = lhs;
//					temp.remove(j);
//					//System.out.println(temp);
//					Set<String> tempClosure = getClosure(temp, FD);
//					//System.out.println("Closure: " + tempClosure.toArray());
//					closureList.add(tempClosure);
//				}
//
//				for (int j = 0; j < closureList.size(); j++){
//					if (closureList.get(j).contains(tmpValue)) {
//						lhs.remove(j);
//					}
//				}
//				leftnew.add(lhs);
//			}else{
//				//add to the left new
//				leftnew.add(lhs);
//			}
//		}
//		// delete redundant FDs from T
//		List<List<String>> basicLHS = new ArrayList<>();
//		List<String> basicRHS = new ArrayList<>();
//
//		for (int i = 0; i < leftnew.size(); i++) {
//			// shape new FD
//			List<List<String>> tempLeft = new ArrayList<>(leftnew);
//			List<String> tempRight = new ArrayList<>(rightnew);
//
//			tempLeft.remove(i);
//			tempRight.remove(i);
//
//			String tempValue = rightnew.get(i);
//			List<String> tempCK = leftnew.get(i);
//
//			Map<List<String>, List<String>> newFD = buildMapfrom2List(tempLeft, tempRight);
//
//			//get closure
//			Set<String> closure = getClosure(tempCK, newFD);
//			if (!closure.contains(tempValue)) {
//				basicLHS.add(tempCK);
//				basicRHS.add(tempValue);
//			}else {
//				leftnew.remove(i);
//				rightnew.remove(i);
//				i--;
//			}
//		}
//
//		basisSchema = buildMapfrom2List(basicLHS, basicRHS);
//		return basisSchema;
//	}
//
//    public static Map<List<String>, List<String>> buildMapfrom2List(List<List<String>> list1, List<String> list2) {
//    	Map<List<String>, List<String>> newFD = new HashMap<>();
//    	for (int i = 0; i < list1.size(); i++) {
//    		if (newFD.containsKey(list1.get(i))){
//                List<String> value = newFD.get(list1.get(i));
//                value.add(list2.get(i));
//                //mapFDs.replace(subsetCK, nonKeys);
//                newFD.replace(list1.get(i), value);
//            }
//            else {
//                newFD.put(list1.get(i), Arrays.asList(list2.get(i)));
//            }
//		}
//    	return newFD;
//	}
    
    public static void main(String[] args) throws IOException{

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

        // Generate the result file
        
        Output.createFile();
        
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
            
            Output.writeResult(tableName);
            Output.writeResult("\t 3NF");
            
            Boolean flag3NF = false;
//            Boolean flag2NF = false;
//            Boolean flag1NF = false;
            
            String explanation = "";
            System.out.println("\nChecking the talble: " + tableName);
            try {
                if (checkTableExist(tableName)) {
                    if (checkColumnAndTableExist(tableName, candidateKey)) {
                        if (checkColumnAndTableExist(tableName, nonKey)) {
                            if (!check1NF_nulls(tableName, candidateKey)) {
                            	explanation = "not in 1NF, null keys\n";
                                System.out.println("Table " + tableName + " not in 1NF: null keys\n");
                            }
                            else if (!check1NF_duplicates(tableName, candidateKey)) {
                            	explanation = "not in 1NF, duplicate keys\n";
                                System.out.println("Table " + tableName + " not in 1NF: duplicate keys\n");
                            }
                            else {
                                partialFD = check2NF(tableName, candidateKey, nonKey);
                                if (!partialFD.isEmpty()) {
                                	explanation = "not in 2NF, ";
                                    System.out.println("Table " + tableName + " not in 2NF\n");
                                    for (Map.Entry<List<String>, List<String>> entry : partialFD.entrySet()) {

                                        for (String key : entry.getKey()) {
                                        	explanation += key + "";
                                            System.out.print(key + " ");
                                        }
                                        explanation += "->";
                                        System.out.print("->");
                                        for (String nKey : entry.getValue()) {
                                            System.out.print(" " + nKey);
                                        	explanation += " " + nKey;
                                        }
                                        System.out.println();
                                        explanation += "; ";
                                    }
                                    // decompose the table
                                    relations = decomposeTo2NF(tableName, candidateKey, nonKey, partialFD);
                                    Boolean flag = decompositionVerify(relations, tableName);
                                    decompositionList.add(relations);
                                    decomposedTableNameList.add(tableName);
                                    decompositionVerification.add(flag);
                                }
                                else {
                                    transitiveFD = check3NF(tableName, nonKey);
                                    if (!transitiveFD.isEmpty()) {
                                        flag3NF = false;
                                    	explanation = "not in 3NF, ";
                                        System.out.println("Table " + tableName + " not in 3NF\n");
                                        for (Map.Entry<List<String>, List<String>> entry : transitiveFD.entrySet()) {
                                        	
                                            for (String key : entry.getKey()) {
                                                System.out.print(key + " ");
                                            	explanation += key + " ";
                                            }
                                            System.out.print("->");
                                            explanation += "->";
                                            for (String nKey : entry.getValue()) {
                                                System.out.print(" " + nKey);
                                            	explanation += " " + nKey;
                                            }
                                            System.out.println();
                                            explanation +="; ";
                                        }
                                        
                                        relations = decomposeTo2NF(tableName, candidateKey, nonKey, transitiveFD);
                                        Boolean flag = decompositionVerify(relations, tableName);
                                        decompositionList.add(relations);
                                        decomposedTableNameList.add(tableName);
                                        decompositionVerification.add(flag);
                                    }
                                    else {
                                    	flag3NF = true;
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
            
            if (flag3NF == true) {
				Output.writeResult("\tY\n");
			}else{
				Output.writeResult("\tN\t" + explanation + "\n");
			}
            
        }

        DbConnection.closeConnection();
        
        // print the decomposition
        printDecomposition(decompositionList, decomposedTableNameList, decompositionVerification);
        
        System.out.println("Done");
    }
}
