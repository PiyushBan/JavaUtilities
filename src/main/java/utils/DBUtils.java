package utils;

import java.sql.*;

public class DBUtils {
    public static String getConnectionString(){
        return "jdbc:mysql://35.154.206.60:3306/!?user=root&password=Byjus@2020";

    }
    public static Connection getConnection(String url) throws SQLException {
        return DriverManager.getConnection(url);
    }

    public static ResultSet executeQuery(String query, Connection conn) throws SQLException {
        Statement st = conn.createStatement();
        ResultSet resultSet = st.executeQuery(query);
        return resultSet;
    }
}
