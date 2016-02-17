package nf;

import java.sql.*;
import java.util.Properties;

public class ConnectionExample {
    private static Statement st = null;

    public static void main(String[] args)  {
        // Load JDBC driver
        try {
            Class.forName("com.vertica.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            // Could not find the driver class. Likely an issue
            // with finding the .jar file.
            System.err.println("Could not find the JDBC driver class.");
            e.printStackTrace();
            return;
        }

        // Create property object to hold username & password
        Properties myProp = new Properties();
        myProp.put("user", "cosc6340");
        myProp.put("password", "1pmMon-Wed");
        Connection conn;
        try {
            conn = DriverManager.getConnection(
                    "jdbc:vertica://129.7.242.19:5433/cosc6340", myProp);
        } catch (SQLException e) {
            // Could not connect to database.
            System.err.println("Could not connect to database.");
            e.printStackTrace();
            return;
        }
        // Connection is established, do something with it here or
        // return it to a calling method
        try {
            st = conn.createStatement();
            ResultSet rs = executeQuery("SELECT * from cosc6340.R1");
            while(rs.next()){
                System.out.println( rs.getInt(1) + " " + rs.getString(2)+ " " + rs.getString(3) + " " + rs.getString(4));
            }
        } catch (SQLException e) {
            System.err.println("Could not create statement");
            e.printStackTrace();
            return;
        }
    }
    public static ResultSet executeQuery(String sqlStatement) {
        try {
            return st.executeQuery(sqlStatement);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
}