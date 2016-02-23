package nf;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.Map;

public class GenerateSQL {

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
                "\tFROM " + tableName + "\n" +
                "WHERE ");

        for (int i = 0; i < candidateKey.size(); i++) {
            sqlQuery.append(candidateKey.get(i) + " IS NULL");
            if (i < candidateKey.size() - 1)
                sqlQuery.append(" OR ");
            else
                sqlQuery.append("; \n");
        }

        writeToFile("\n -- Check nulls in candidate key \n");
        writeToFile(sqlQuery.toString());

        return sqlQuery.toString();
    }

    public static String getTotalCount(String tableName) {

        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append("SELECT COUNT(*) \n" +
                "\tFROM " + tableName + ";\n");

        writeToFile("\n -- Get total rows count \n");
        writeToFile(sqlQuery.toString());

        return sqlQuery.toString();
    }

    public static String getDistinctCount(String tableName, List<String> candidateKey) {

        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append("SELECT COUNT(*) \n" +
                "\tFROM \n" +
                "\t(SELECT DISTINCT ");

        for (int i = 0; i < candidateKey.size(); i++) {
            sqlQuery.append(candidateKey.get(i));
            if (i < candidateKey.size() - 1)
                sqlQuery.append(", ");
            else
                sqlQuery.append("\n");
        }

        sqlQuery.append("\t\tFROM " + tableName + ") T1;\n");

        writeToFile("\n -- Get distinct count \n");
        writeToFile(sqlQuery.toString());

        return sqlQuery.toString();
    }

    public static String getDistinctCount(String tableName, String candidateKey) {

        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append("SELECT COUNT(DISTINCT " + candidateKey + ") \n" +
                "\tFROM " + tableName + ";\n");

        writeToFile("\n -- Get distinct count \n");
        writeToFile(sqlQuery.toString());

        return sqlQuery.toString();
    }
    
    public static String getTableExist(String tableName) {
		StringBuilder sqlQuery = new StringBuilder();
		sqlQuery.append("SELECT COUNT(*) \n" +
				"\tFROM Tables \n" + 
				"\tWHERE table_name=\'"+tableName + "\';\n");
		
		writeToFile("\n -- Get queried table\n");
		writeToFile(sqlQuery.toString());
		
		return sqlQuery.toString();
	}
    
    public static String getColumnAndTableExist(String tableName, String columnName) {
		StringBuilder sqlQuery = new StringBuilder();
		sqlQuery.append("SELECT COUNT(*) \n" + 
				"\tFROM Columns \n" +
				"\tWHERE column_name=\'"+columnName+"\' AND table_name=\'"+tableName+"\';\n");
		
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

        sqlQuery.append("\t\tFROM " + originalTable + ";\n");

        writeToFile("\n -- Create local temporary table \n");
        writeToFile(sqlQuery.toString());

        return sqlQuery.toString();
    }
}
