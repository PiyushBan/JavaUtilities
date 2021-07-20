package helpers;

import utils.DBUtils;
import utils.FileUtils;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class SfArchival_DateTimeMigrator {
    public static Map<Integer, Integer> typeOfDBColumn = new HashMap<Integer, Integer>();

    public static void getTableDetails(String Tablename, String dbUrl) throws SQLException, IOException {
        String descSql = "DESC @";
        descSql = descSql.replaceAll("@", Tablename);
        Connection conn = DBUtils.getConnection(dbUrl);
        Statement st1 = conn.createStatement();
        ResultSet rst = st1.executeQuery(descSql);

        //getting types of DB columns by desc SQL
        //0 denotes String and 1 denotes INT
        Integer indexOfColumn = 1;

        while (rst.next()) {
            String typeColumn = rst.getString(2);
            if (typeColumn.substring(0, 3).equals("int")) {
                typeOfDBColumn.put(indexOfColumn, 1);
            } else {
                typeOfDBColumn.put(indexOfColumn, 0);
            }
            indexOfColumn++;
        }

        st1.close();
        rst.close();
        conn.close();

    }

    public static void migrator(String dbName, String TableName, int y, int m, int d, int maxYear) throws SQLException, IOException {
        Map<Integer, Integer> days = FileUtils.calendar();
        String dbUrl = DBUtils.getConnectionString();
        dbUrl = dbUrl.replaceAll("!", dbName);
        String destFile = "/Users/piyushbansal/Documents/&/Migration/@/Activity#.csv";
        String sql = "Select * from @ " +
                "where ModifiedOn between '#' and '# 23:59:59'";
        getTableDetails(TableName, dbUrl);
        destFile = destFile.replaceAll("@", TableName);
        destFile = destFile.replaceAll("&", dbName);
        sql = sql.replaceAll("@", TableName);


        int fileCounter = 0, counter = 0, globalCounter = 0;
        int year = y, month = m, date = d;
        StringBuilder output = new StringBuilder();
        Connection conn = DBUtils.getConnection(dbUrl);
        do {
            String dateStr = year + "-" + month + "-" + date;
            String tempSql = sql.replaceAll("#", dateStr);
            //            System.out.println("Executing "+ tempSql+" ...");

            ResultSet rs = DBUtils.executeQuery(tempSql, conn);
            ResultSetMetaData rsmd = rs.getMetaData();

            int noOfColumns = rsmd.getColumnCount();

            while (rs.next()) {
                for (int i = 0; i < noOfColumns; i++) {
                    String value = rs.getString((i + 1));
                    boolean nullFlag = false;
                    if (value==null) {
                        nullFlag = true;
                    }
                    if (i == 0) {
                        if (nullFlag) {
                            Integer typeOfColumn = typeOfDBColumn.get(i + 1);
                            if (typeOfColumn == 1) output.append(0);
                            else output.append("");
                        } else {
                            output.append(rs.getString(i + 1));
                        }
                    } else {
                        output.append("|");

                        if (nullFlag) {
                            Integer typeOfColumn = typeOfDBColumn.get(i + 1);
                            if (typeOfColumn == 1) output.append(0);
                            else output.append("");

                        }
                        else
                            output.append(rs.getString(i + 1));
                    }
                }
                output.append("\n");
                counter++;
                globalCounter++;

            }
            rs.close();
            if (counter >= 600000) {
                fileCounter++;
                String tempDstFile = destFile.replaceAll("#", String.valueOf(fileCounter));
                FileUtils.writeFile(tempDstFile, output.toString());
                System.out.println(globalCounter + " Rows Saved in file number " + fileCounter + "... ");
                counter = 0;
                output = new StringBuilder();
            }
            int[] updatedDate = getDate(year, month, date, days);
            year = updatedDate[0];
            month = updatedDate[1];
            date = updatedDate[2];

        } while (year <= maxYear);
        if (counter != 0) {
            fileCounter++;
            String tempDstFile = destFile.replaceAll("#", String.valueOf(fileCounter));
            FileUtils.writeFile(tempDstFile, output.toString());
            System.out.println(globalCounter + " Rows Saved in file number " + fileCounter + "... ");
            counter = 0;

        }
    }

    public static void specialMigrator(String dbName, String TableName, int y, int m, int d, int maxYear) throws SQLException, IOException {
        Map<Integer, Integer> days = FileUtils.calendar();
        String dbUrl = DBUtils.getConnectionString();
        dbUrl = dbUrl.replaceAll("!", dbName);
        String destFile = "/Users/piyushbansal/Documents/&/Migration/@/Activity#.csv";
        String sql = "Select * from @ " +
                "where lastModifiedOn between '#' and '# 23:59:59'";

        destFile = destFile.replaceAll("@", TableName);
        destFile = destFile.replaceAll("&", dbName);
        sql = sql.replaceAll("@", TableName);
        getTableDetails(TableName, dbUrl);


        int fileCounter = 0, counter = 0, globalCounter = 0;
        int year = y, month = m, date = d;
        StringBuilder output = new StringBuilder();
        Connection conn = DBUtils.getConnection(dbUrl);
        do {
            String dateStr = year + "-" + month + "-" + date;
            String tempSql = sql.replaceAll("#", dateStr);
            //            System.out.println("Executing "+ tempSql+" ...");

            ResultSet rs = DBUtils.executeQuery(tempSql, conn);
            ResultSetMetaData rsmd = rs.getMetaData();

            int noOfColumns = rsmd.getColumnCount();

            while (rs.next()) {
                for (int i = 0; i < noOfColumns; i++) {
                    String value = rs.getString((i + 1));
                    boolean nullFlag = false;
                    if (value==null) {
                        nullFlag = true;
                    }
                    if (i == 0) {
                        if (nullFlag) {
                            Integer typeOfColumn = typeOfDBColumn.get(i + 1);
                            if (typeOfColumn == 1) output.append(0);
                            else output.append("");
                        } else {
                            output.append(rs.getString(i + 1));
                        }
                    } else {
                        output.append("|");

                        if (nullFlag) {
                            Integer typeOfColumn = typeOfDBColumn.get(i + 1);
                            if (typeOfColumn == 1) output.append(0);
                            else output.append("");

                        }
                        else output.append(rs.getString(i + 1));
                    }

                }
                output.append("\n");
                counter++;
                globalCounter++;
            }
            rs.close();
            if (counter >= 600000) {
                fileCounter++;
                String tempDstFile = destFile.replaceAll("#", String.valueOf(fileCounter));
                FileUtils.writeFile(tempDstFile, output.toString());
                System.out.println(globalCounter + " Rows Saved in file number " + fileCounter + "... ");
                counter = 0;
                output = new StringBuilder();
            }
            int[] updatedDate = getDate(year, month, date, days);
            year = updatedDate[0];
            month = updatedDate[1];
            date = updatedDate[2];

        } while (year <= maxYear);
        if (counter != 0) {
            fileCounter++;
            String tempDstFile = destFile.replaceAll("#", String.valueOf(fileCounter));
            FileUtils.writeFile(tempDstFile, output.toString());
            System.out.println(globalCounter + " Rows Saved in file number " + fileCounter + "... ");
            counter = 0;

        }
    }

    public static int[] getDate(int y, int m, int d, Map<Integer, Integer> days) {

        int[] ans = new int[3];
        d = d + 1;
        if (m == 2 && isLeapYear(y)) {
            if (d == 30) {
                m = m + 1;
                d = 1;
            }
        } else {
            if (days.get(m) < d) {
                m = m + 1;
                if (m > 12) {
                    y = y + 1;
                    m = 1;
                }
                d = 1;
            }
        }
        //        System.out.println(y+"-"+m+"-"+d);
        ans[0] = y;
        ans[1] = m;
        ans[2] = d;
        return ans;
    }

    public static boolean isLeapYear(int year) {
        return ((year % 4 == 0) && (year % 100 != 0)) || (year % 400 == 0);
    }

}
