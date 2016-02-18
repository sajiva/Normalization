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
        }

        return true;
    }
    
    // Check the table name exist in the database
    public static boolean checkTableExist(String tablename){
    	String sqlQuery = GenerateSQL.getTableExist(tablename);
    	System.out.println(sqlQuery);
    	ResultSet rs = DbConnection.executeQuery(sqlQuery);
    	try {
    		if (rs.next()) {
    			if (0 ==rs.getInt(1)) {
    				System.err.println("Cannot find the table " + tablename);
    				return false;
    			}
    			else {
    				System.out.println("Table " + tablename + " exists");
    			}
			}	
		} catch (SQLException e) {
			System.out.println("Wrong in checkTableExist!");
			e.printStackTrace();
		}
		return true;
	}
    
    // Check Column exist
    public static boolean checkColumnAndTableExist(String tablename, String columnname) {
    	String sqlQuery = GenerateSQL.getColumnAndTableExist(tablename, columnname);
    	System.out.println(sqlQuery);
    	ResultSet rs = DbConnection.executeQuery(sqlQuery);
    	try {
    		if (rs.next()) {
    			if (0 == rs.getInt(1)) {
    				System.err.println("Cannot find the column "+ columnname +" in table " + tablename);
    				return false;
    			}
    			else {
    				System.out.println("Column " + columnname + " exists in table " + tablename);
    			}
			}	
		} catch (SQLException e) {
			System.out.println("Wrong in checkTableExist!");
			e.printStackTrace();
		}
		return true;
	}
    
    
    public static void main(String[] args) {
    	
    	//Check database connection
        if (!DbConnection.connect())
            return;

        //Check input database/table
        ArgPaser.parse(args);
        ArgPaser.readFile();
        
        List<String> tableNames = ArgPaser.tableNames;
        List<List<String>> candidateKey = ArgPaser.candidateKeys;
        List<List<String>> nonKeyAttributes = ArgPaser.nonKeyAttributes;
        
        //Check table;
        for(String tbname: tableNames){
        	if (!checkTableExist(tbname)) {
				System.err.println("The table " + tbname + " is not existed");
			}
        }
        
        //Check attribute
        for(int i = 0; i < tableNames.size(); i++){
        	String tablename = tableNames.get(i);
        	List<String> ck = candidateKey.get(i);
        	List<String> nk = nonKeyAttributes.get(i);
        	
        	for (int j = 0; j < ck.size(); j++) {
        		String columnname = ck.get(j);
				if (!checkColumnAndTableExist(tablename, columnname)) {
					System.err.println("The column " + columnname + " in table " + tablename + " is not existed");
				}
			}
        	
        	for (int j = 0; j < nk.size(); j++) {
        		String columnname = nk.get(j);
				if (!checkColumnAndTableExist(tablename, columnname)) {
					System.err.println("The column " + columnname + " in table " + tablename + " is not existed");
				}
			}
        }
        /*
        String tableName = "R3";
        List<String> candidateKey = Arrays.asList("N");
        List<String> nonKeyAttributes = Arrays.asList("M", "C");
        */
        
        try {
        	for (int i = 0; i < tableNames.size(); i++) {
        		if (!check1NF_nulls(tableNames.get(i), candidateKey.get(i))) {
                    System.out.println("Table not in 1NF: null keys\n");
                }
                else if (!check1NF_duplicates(tableNames.get(i), candidateKey.get(i))) {
                    System.out.println("Table not in 1NF: duplicate keys\n");
                }
                else if (!check2NF(tableNames.get(i), candidateKey.get(i), nonKeyAttributes.get(i))){
                    System.out.println("Table not in 2NF\n");
                }
                else if (!check3NF(tableNames.get(i), nonKeyAttributes.get(i))) {
                    System.out.println("Table not in 3NF\n");
                }
                else {
                    System.out.println("Table is in 3NF\n");
                }
			}
            
        } catch (SQLException e) {
            System.err.println("Could not execute query");
            e.printStackTrace();
            return;
        }
        

//        tableName = "R2";
//        candidateKey = Arrays.asList("N");
//        try {
//            if (!check1NF_nulls(tableName, candidateKey)) {
//                System.out.println("Table not in 1NF: null keys\n");
//            }
//            else if (!check1NF_duplicates(tableName, candidateKey)) {
//                System.out.println("Table not in 1NF: duplicate keys\n");
//            }
//            else if (!check2NF(tableName, candidateKey, nonKeyAttributes)){
//                System.out.print("Table not in 2NF\n");
//            }
//            else {
//                System.out.println("Table is in 2NF\n");
//            }
//        } catch (SQLException e) {
//            System.err.println("Could not execute query");
//            e.printStackTrace();
//            return;
//        }

        DbConnection.closeConnection();


    }


}
