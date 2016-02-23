package nf;

import org.junit.Test;

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

}