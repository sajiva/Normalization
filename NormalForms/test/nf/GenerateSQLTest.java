package nf;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

public class GenerateSQLTest {

    @Test
    public void testBuildQueryCheckNulls1() {
        String query = "SELECT COUNT(*) \n" +
                "\tFROM R1\n" +
                "WHERE N IS NULL OR A IS NULL OR B IS NULL; \n";
        assertEquals(GenerateSQL.checkNulls("R1", Arrays.asList("N", "A", "B")), query);
    }

    @Test
    public void testBuildQueryCheckNulls2() {
        String query = "SELECT COUNT(*) \n" +
                "\tFROM R2\n" +
                "WHERE N IS NULL; \n";
        assertEquals(GenerateSQL.checkNulls("R2", Arrays.asList("N")), query);
    }

    @Test
    public void testBuildQueryGetTotalCount() {
        String query = "SELECT COUNT(*) \n" +
                "\tFROM R1;\n";

        assertEquals(GenerateSQL.getTotalCount("R1"), query);
    }

    @Test
    public void testBuildQueryGetDistinctCount1() {
        String query = "SELECT COUNT(*) \n" +
                "\tFROM \n" +
                "\t(SELECT DISTINCT N, A, B\n" +
                "\t\tFROM R1) T1;\n";

        assertEquals(GenerateSQL.getDistinctCount("R1", Arrays.asList("N", "A", "B")), query);
    }

    @Test
    public void testBuildQueryGetDistinctCount2() {
        String query = "SELECT COUNT(DISTINCT A) \n" +
                "\tFROM R2;\n";

        assertEquals(GenerateSQL.getDistinctCount("R2", "A"), query);
    }

    @Test
    public void testBuildQueryCreateTempTable() {
        String query = "CREATE LOCAL TEMP TABLE R11 ON COMMIT PRESERVE ROWS AS\n" +
                "\tSELECT DISTINCT N, A, B\n" +
                "\t\tFROM R1;\n";
        assertEquals(GenerateSQL.createTempTable("R1", "R11", Arrays.asList("N", "A", "B")), query);
    }

    @Test
    public void testBuildQueryGetCountJoinTables() {
        String query = "SELECT COUNT(*)\n" +
                "FROM R_1\n" +
                "JOIN R_2\n" +
                "\tON R_1.A = R_2.A\n" +
                "JOIN R_3\n" +
                "\tON R_1.B = R_3.B\n" +
                "\tAND R_1.C = R_3.C\n";
        assertEquals(GenerateSQL.getCountJoinTables(Arrays.asList("R_1", "R_2", "R_3"), Arrays.asList(Arrays.asList("A"), Arrays.asList("B", "C"))), query);
    }

}