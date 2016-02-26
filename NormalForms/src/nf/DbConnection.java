package nf;
/**********************************************************************************************/
/* COSC6340: Database Systems                                                                 */
/* Project: Discovering Functional Dependencies and Certifying Normal Forms with SQL Queries  */
/* Project team: Sajiva Pradhan (1007766), Xiang Xu (1356333)                                 */
/**********************************************************************************************/

import java.sql.*;
import java.util.Properties;

// Connect to the Vertica DB and execute SQL queries
public class DbConnection {

    private static Connection conn;

    public static boolean connect() {
        // Load JDBC driver
        try {
            Class.forName("com.vertica.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("Could not find the JDBC driver class.");
            e.printStackTrace();
            return false;
        }

        // Create property object to hold username & password
        Properties myProp = new Properties();
        myProp.put("user", "cosc6340");
        myProp.put("password", "1pmMon-Wed");

        try {
            conn = DriverManager.getConnection(
                    "jdbc:vertica://129.7.242.19:5433/cosc6340", myProp);
        } catch (SQLException e) {
            System.err.println("Could not connect to database.");
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static ResultSet executeQuery(String sqlStatement) {
        Statement st = null;
        try {
            st = conn.createStatement();
            return st.executeQuery(sqlStatement);
        } catch (SQLException e) {
            System.err.println("Could not create statement");
            e.printStackTrace();
            return null;
        }
    }

    public static void execute(String sqlStatement) {
        Statement st = null;
        try {
            st = conn.createStatement();
            st.execute(sqlStatement);
        } catch (SQLException e) {
            System.err.println("Could not create statement");
            e.printStackTrace();
        }
    }

    public static void closeConnection() {
        try {
            conn.close();
        } catch (SQLException e) {
            System.err.println("Could not close database connection");
            e.printStackTrace();
        }
    }
}
