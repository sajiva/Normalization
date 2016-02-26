package nf;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;

public class GenerateSQL {

	public static void createFile() throws IOException{
		FileWriter writer = new FileWriter("NF.sql");
		writer.close();
	}

    public static void writeToFile(String sqlQuery) {
        try {
            Files.write(Paths.get("NF.sql"), sqlQuery.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("Error writing to file");
            e.printStackTrace();
        }
    }

    public static String checkNulls(String tableName, List<String> candidateKey) {

        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append("SELECT COUNT(*) \n" +
                "FROM " + tableName + "\n" +
                "WHERE ");

        for (int i = 0; i < candidateKey.size(); i++) {
            sqlQuery.append(candidateKey.get(i) + " IS NULL\n");
            if (i < candidateKey.size() - 1)
                sqlQuery.append("\tOR ");
        }

        writeToFile("\n -- Check nulls in candidate key \n");
        writeToFile(sqlQuery.toString());

        return sqlQuery.toString();
    }

    public static String getTotalCount(String tableName) {

        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append("SELECT COUNT(*) \n" +
                "FROM " + tableName + ";\n");

        writeToFile("\n -- Get total rows count \n");
        writeToFile(sqlQuery.toString());

        return sqlQuery.toString();
    }

    public static String getDistinctCount(String tableName, List<String> candidateKey) {

        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append("SELECT COUNT(*) \n" +
                "FROM \n" +
                "\t(SELECT DISTINCT ");

        for (int i = 0; i < candidateKey.size(); i++) {
            sqlQuery.append(candidateKey.get(i));
            if (i < candidateKey.size() - 1)
                sqlQuery.append(", ");
            else
                sqlQuery.append("\n");
        }

        sqlQuery.append("\tFROM " + tableName + ") T1;\n");

        writeToFile("\n -- Get distinct count \n");
        writeToFile(sqlQuery.toString());

        return sqlQuery.toString();
    }

    public static String getDistinctCount(String tableName, String candidateKey) {

        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append("SELECT COUNT(DISTINCT " + candidateKey + ") \n" +
                "FROM " + tableName + ";\n");

        writeToFile("\n -- Get distinct count \n");
        writeToFile(sqlQuery.toString());

        return sqlQuery.toString();
    }
    
    public static String getTableExist(String tableName) {

		StringBuilder sqlQuery = new StringBuilder();
		sqlQuery.append("SELECT COUNT(*) \n" +
				"FROM Tables \n" +
				"WHERE table_name = \'"+tableName + "\';\n");
		
		writeToFile("\n -- Get queried table\n");
		writeToFile(sqlQuery.toString());
		
		return sqlQuery.toString();
	}
    
    public static String getColumnAndTableExist(String tableName, String columnName) {

		StringBuilder sqlQuery = new StringBuilder();
		sqlQuery.append("SELECT COUNT(*) \n" + 
				"FROM Columns \n" +
				"WHERE column_name = \'"+columnName+"\'\n" +
                "\tAND table_name = \'"+tableName+"\';\n");
		
		writeToFile("\n -- Check table and columns\n");
		writeToFile(sqlQuery.toString());
		
		return sqlQuery.toString();
	}

    public static String createTempTable(String originalTable, String tempTable, List<String> attributesList) {

        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append("CREATE LOCAL TEMP TABLE " + tempTable + " ON COMMIT PRESERVE ROWS AS\n" +
                "\tSELECT DISTINCT ");

        for (int i = 0; i < attributesList.size(); i++) {
            sqlQuery.append(attributesList.get(i));
            if (i < attributesList.size() - 1)
                sqlQuery.append(", ");
            else
                sqlQuery.append("\n");
        }

        sqlQuery.append("\tFROM " + originalTable + ";\n");

        writeToFile("\n -- Create local temporary table \n");
        writeToFile(sqlQuery.toString());

        return sqlQuery.toString();
    }

    public static String getCountJoinTables(List<String> tableNames, List<List<String>> foreignKeys) {

        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append("SELECT COUNT(*)\n" +
                "FROM " + tableNames.get(0) + "\n");
                for (int i = 1; i < tableNames.size(); i++) {
                    sqlQuery.append("JOIN " + tableNames.get(i) + "\n\tON ");

                    List<String> fkeys = foreignKeys.get(i-1);
                    for (int j = 0; j < fkeys.size(); j++) {
                        sqlQuery.append(tableNames.get(0) + "." + fkeys.get(j) + " = " + tableNames.get(i) + "." + fkeys.get(j) + "\n");
                        if (j < fkeys.size() - 1) {
                            sqlQuery.append("\tAND ");
                        }
                    }
                }

        writeToFile("\n -- Join two tables and get count \n");
        writeToFile(sqlQuery.toString());

        return sqlQuery.toString();
    }
}
