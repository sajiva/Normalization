package nf;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class DbConnectionTest {

    @Before
    public void setup() {
        DbConnection.connect();
    }

    @After
    public void close() {
        DbConnection.closeConnection();
    }

    @Test
    public void testExecuteQuery() {
        String query = "SELECT count(*) \n" +
                "  FROM R1\n" +
                "WHERE N=null OR A=null OR B=null;";
        ResultSet rs = DbConnection.executeQuery(query);
        int result = -1;
        try {
            if (rs.next())
                result = rs.getInt(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        assertEquals(result, 0);
    }

}