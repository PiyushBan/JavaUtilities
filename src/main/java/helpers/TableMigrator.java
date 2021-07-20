package helpers;

import com.mysql.cj.protocol.Resultset;
import utils.DBUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLOutput;

public class TableMigrator {
    public static void migrator(String dbName, String tableName) throws SQLException, IOException {
        String sql = "Select count(*) from @ ";
        sql = sql.replaceAll("@", tableName);
        String url = DBUtils.getConnectionString();
        url = url.replaceAll("!", dbName);
        Connection conn = DBUtils.getConnection(url);

        System.out.println("Database " + dbName + " Connected Successfully");

        ResultSet rs = DBUtils.executeQuery(sql, conn);
        rs.next();
        int noOfRows = rs.getInt(1);
        System.out.println("Total no of records are " + noOfRows);

        if (noOfRows >= 600000) {
            //Data to be divided in partition of above value;
            int y, m, d, maxYear;
            sql = "Select modifiedon, count(*) from @ group by modifiedon order by modifiedon asc "+
            "LIMIT 1";
            sql = sql.replaceAll("@", tableName);
            rs = DBUtils.executeQuery(sql, conn);
            rs.next();
            String dateString = rs.getString(1);
            String[] dateWithoutTime = dateString.split(" ");
            String[] dateParam = dateWithoutTime[0].split("-");
            y = Integer.parseInt(dateParam[0]);
            m = Integer.parseInt(dateParam[1]);
            d = Integer.parseInt(dateParam[2]);
            sql = "Select modifiedon, count(*) from @ group by modifiedon order by modifiedon desc " +
                    "LIMIT 1";
            sql = sql.replaceAll("@", tableName);
            rs = DBUtils.executeQuery(sql, conn);
            rs.next();
            dateString = rs.getString(1);
            dateWithoutTime = dateString.split(" ");
            dateParam = dateWithoutTime[0].split("-");
            maxYear = Integer.parseInt(dateParam[0]);
            maxYear++;
//            System.out.println(y+" "+m+" "+" "+ d+" "+ maxYear);
            SfArchival_DateTimeMigrator.migrator(dbName, tableName, y, m, d, maxYear);
        } else {
            //Simple Migrator used. No partitioning of data
            SfArchivalMigratorHelper.migrator(dbName, tableName);
        }

    }

    public static void specialMigrator(String dbName, String tableName) throws SQLException, IOException {
        String sql = "Select count(*) from @";
        sql = sql.replaceAll("@", tableName);
        String url = DBUtils.getConnectionString();
        url = url.replaceAll("!", dbName);
        Connection conn = DBUtils.getConnection(url);

        System.out.println("Database " + dbName + " Connected Successfully");

        ResultSet rs = DBUtils.executeQuery(sql, conn);
        rs.next();
        int noOfRows = rs.getInt(1);
        System.out.println("Total no of records are " + noOfRows);

        if (noOfRows >= 600000) {
            //Data to be divided in partition of above value;
            int y, m, d, maxYear;
            sql = "Select lastmodifiedon, count(*) from @ group by lastmodifiedon order by modifiedon asc "+
                    "LIMIT 1";
            sql = sql.replaceAll("@", tableName);
            rs = DBUtils.executeQuery(sql, conn);
            rs.next();
            String dateString = rs.getString(1);
            String[] dateWithoutTime = dateString.split(" ");
            String[] dateParam = dateWithoutTime[0].split("-");
            y = Integer.parseInt(dateParam[0]);
            m = Integer.parseInt(dateParam[1]);
            d = Integer.parseInt(dateParam[2]);
            sql = "Select lastmodifiedon, count(*) from @ group by lastmodifiedon order by lastmodifiedon desc " +
                    "LIMIT 1";
            sql = sql.replaceAll("@", tableName);
            rs = DBUtils.executeQuery(sql, conn);
            rs.next();
            dateString = rs.getString(1);
            dateWithoutTime = dateString.split(" ");
            dateParam = dateWithoutTime[0].split("-");
            maxYear = Integer.parseInt(dateParam[0]);
            maxYear++;
//            System.out.println(y+" "+m+" "+" "+ d+" "+ maxYear);
            SfArchival_DateTimeMigrator.migrator(dbName, tableName, y, m, d, maxYear);
        } else {
            //Simple Migrator used. No partitioning of data
            SfArchivalMigratorHelper.migrator(dbName, tableName);
        }

    }

}
