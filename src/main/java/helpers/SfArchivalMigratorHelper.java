package helpers;

import utils.DBUtils;
import utils.FileUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

public class SfArchivalMigratorHelper {
    public static void migrator(String dbName,String TableName) throws SQLException, IOException {
        String dbUrl = DBUtils.getConnectionString();
        dbUrl=dbUrl.replaceAll("!",dbName);
        String destFile = "/Users/piyushbansal/Documents/&/Migration/@/File#.csv";
        String sql = "Select * from @ ";

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
                if (i == 0) {
                    output.append(rs.getString(i + 1));
                } else {
                    output.append("|");
                    output.append(rs.getString(i + 1));
                }
            }
            globalCounter++;
            counter++;
            output.append("\n");
            String s=new String(output);
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
