package nf;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class CertifyNFTest {

    @Before
    public void setup() {
        DbConnection.connect();
    }

    @After
    public void close() {
        DbConnection.closeConnection();
    }

    @Test
    public void testCheck1NF_nulls() {

        try {
            assertTrue(CertifyNF.check1NF_nulls("R1", Arrays.asList("N", "A", "B")));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCheck1NF_nulls2() {

        try {
            assertTrue(CertifyNF.check1NF_nulls("R2", Arrays.asList("N")));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCheck1NF_duplicates() {

        try {
            assertTrue(CertifyNF.check1NF_duplicates("R1", Arrays.asList("N", "A", "B")));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCheck1NF_duplicates2() {

        try {
            assertFalse(CertifyNF.check1NF_duplicates("R1", Arrays.asList("A")));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCheck1NF_duplicates3() {

        try {
            assertTrue(CertifyNF.check1NF_duplicates("R2", Arrays.asList("N")));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCheck2NF() {
        try {
            Map<List<String>,List<String>> mapFD = new HashMap<>();
            assertEquals(CertifyNF.check2NF("R2", Arrays.asList("N"), Arrays.asList("M", "C")), mapFD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCheck2NF2() {
        try {
            Map<List<String>,List<String>> mapFD = new HashMap<>();
            mapFD.put(Arrays.asList("B"), Arrays.asList("C"));
            mapFD.put(Arrays.asList("A", "B"), Arrays.asList("C"));
            mapFD.put(Arrays.asList("B", "N"), Arrays.asList("C"));
            assertEquals(CertifyNF.check2NF("R1", Arrays.asList("N", "A", "B"), Arrays.asList("C")), mapFD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCheck3NF() {
        try {
            assertTrue(CertifyNF.check3NF("R1", Arrays.asList("C")));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCheck3NF2() {
        try {
            assertFalse(CertifyNF.check3NF("R2", Arrays.asList("M", "C")));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCheck3NF3() {
        try {
            assertTrue(CertifyNF.check3NF("R3", Arrays.asList("M", "C")));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}