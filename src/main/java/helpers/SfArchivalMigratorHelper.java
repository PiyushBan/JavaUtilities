package helpers;

import utils.DBUtils;
import utils.FileUtils;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class SfArchivalMigratorHelper {
    public static Map<Integer, Integer> typeOfDBColumn=new HashMap<Integer,Integer>() ;
    public static void getTableDetails(String Tablename,String dbUrl) throws SQLException,IOException{
        String descSql="DESC @";
        descSql=descSql.replaceAll("@",Tablename);
        Connection conn = DBUtils.getConnection(dbUrl);
        Statement st1=conn.createStatement();
        ResultSet rst = st1.executeQuery(descSql);

        //getting types of DB columns by desc SQL
        //0 denotes String and 1 denotes INT
        Integer indexOfColumn = 1;

        while (rst.next()) {
            String typeColumn = rst.getString(2);
            if (typeColumn.substring(0, 3) == "int") {
                typeOfDBColumn.put(indexOfColumn, 1);
            } else {
                typeOfDBColumn.put(indexOfColumn, 0);
            }
            indexOfColumn++;
        }
        rst.close();
        conn.close();

    }

    public static void migrator(String dbName,String TableName) throws SQLException, IOException {
        String dbUrl = DBUtils.getConnectionString();
        dbUrl=dbUrl.replaceAll("!",dbName);
        String destFile = "/Users/piyushbansal/Documents/&/Migration/@/File#.csv";
        String sql = "Select * from @ ";
        getTableDetails(TableName,dbUrl);
        ;
        sql=sql.replaceAll("@",TableName);
        destFile = destFile.replaceAll("@", TableName);
        destFile=destFile.replaceAll("&",dbName);

        int fileCounter = 0, counter = 0, globalCounter = 0;
        StringBuilder output = new StringBuilder();
        Connection conn = DBUtils.getConnection(dbUrl);
        ResultSet rs = DBUtils.executeQuery(sql, conn);
        ResultSetMetaData rsmd = rs.getMetaData();

        int noOfColumns = rsmd.getColumnCount();

        while (rs.next()) {
            for (int i = 0; i < noOfColumns; i++) {
                String value = rs.getString((i + 1));
                boolean nullFlag = false;
                if (value == "null") {
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
                output.append("\n");
                counter++;
                globalCounter++;

            }
        }
        rs.close();
        if (counter != 0) {
            fileCounter++;
            String tempDstFile = destFile.replaceAll("#", String.valueOf(fileCounter));
            FileUtils.writeFile(tempDstFile, output.toString());
            System.out.println(globalCounter + " Rows Saved in file number " + fileCounter + "... ");
            counter = 0;
            output = new StringBuilder();
        }
    }


}
