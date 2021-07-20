package helpers;

import utils.DBUtils;
import utils.FileUtils;

import java.io.IOException;
import java.sql.*;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class byjusSfArchivalMigrator {
    public static void sfArchivalDataMigrator() throws SQLException, IOException {
        String dbUrl = DBUtils.getConnectionString();
        String destFile = "/Users/piyushbansal/Documents/Archival/Migration/7461_Activity/activity#.txt";
        String sql = "select * from 7461_ActivityTypes";

        int fileCounter = 0;
        StringBuilder output = new StringBuilder();
        int counter = 0;
        Connection conn = DBUtils.getConnection(dbUrl);
        System.out.println("Getting data from DB via: " + sql);

        ResultSet resultSet = DBUtils.executeQuery(sql, conn);
        while (resultSet.next()) {
            if(counter==0){
                output.append("ActivityType").append(";").append("ActivityEventName").append(";").append("DisplayName").append(";")
                        .append("StatusCode").append(";").append("attributes").append("\n");
            }
            String ActivityType = resultSet.getString("ActivityType");
            String ActivityEventName = resultSet.getString("ActivityEventName");
            String DisplayName = resultSet.getString("DisplayName");
            String StatusCode = resultSet.getString("StatusCode");
            String attributes = resultSet.getString("attributes");

            output.append(ActivityType).append(";").append(ActivityEventName).append(";").append(DisplayName).append(";")
                    .append(StatusCode).append(";").append(attributes).append("\n");
            counter++;
            if (counter >= 600000) {
                fileCounter++;
                String tempDestFile = destFile.replaceAll("#", String.valueOf(fileCounter));
                System.out.println("File no "+fileCounter+ " created successfully.");
                FileUtils.writeFile(tempDestFile,output.toString());
                output=new StringBuilder();
                counter=0;
            }
        }
        if(counter!=0){
            fileCounter++;
            String tempDestFile = destFile.replaceAll("#", String.valueOf(fileCounter));
            System.out.println("File no "+fileCounter+ " created successfully.");
            FileUtils.writeFile(tempDestFile,output.toString());
            output=new StringBuilder();
            counter=0;
        }
    }
}
