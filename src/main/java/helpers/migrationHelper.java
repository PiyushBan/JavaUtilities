package helpers;

import utils.DBUtils;
import utils.FileUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class migrationHelper {

    public static void eScoreMigrator() throws SQLException, IOException {

        Map<Integer, Integer> days = FileUtils.calendar();

        String dbUrl = "jdbc:mysql://35.154.206.60:3306/byjus_sf_archival?user=root&password=Byjus@2020";
        String destFile="/Users/piyushbansal/Documents/Archival/Migration/7461_Activity/score#.csv";
        String sql = "select PremiumID, AppID, EngagementDate, EngagementScore, PrimaryOrSecondary, LastModifiedOn " +
                "from Engagement_Score where EngagementDate = '#'";
        int fileCounter = 0;
        int year = 2020, month = 1, date = 1;
        StringBuilder output = new StringBuilder();
        int counter = 0;
        Connection conn = DBUtils.getConnection(dbUrl);
        do {

            String dateStr = year + "-" + month + "-" + date;
            String tmpSql = sql.replaceAll("#", dateStr);

            System.out.println("executing : " + tmpSql);

            ResultSet resultSet = DBUtils.executeQuery(tmpSql, conn);


            while (resultSet.next()) {
                String premiumId = resultSet.getString("PremiumId");
                String appId = resultSet.getString("AppID");
                String engagementDate = resultSet.getString("EngagementDate");
                String engagementScore = resultSet.getString("EngagementScore");
                String isPrimary = resultSet.getString("PrimaryOrSecondary");
                String lastModifiedDate = resultSet.getString("LastModifiedOn");

                output.append(premiumId).append("|").append(appId).append("|").append(engagementDate)
                        .append("|").append(engagementScore).append("|").append(isPrimary).append("|")
                        .append(lastModifiedDate).append("\n");
                counter ++;
            }
            resultSet.close();

            if (counter >= 500000) {
                String tmpDestFile = destFile.replaceAll("#", String.valueOf(fileCounter));
                fileCounter += 1;
                FileUtils.writeFile(tmpDestFile, output.toString());
                output = new StringBuilder();
                counter = 0;
            }

            int[] updatedDate = getDate(year, month, date, days);
            year = updatedDate[0];
            month = updatedDate[1];
            date = updatedDate[2];

        } while (year <= 2021);

        if (counter > 0) {
            String tmpDestFile = destFile.replaceAll("#", String.valueOf(fileCounter));
            FileUtils.writeFile(tmpDestFile, output.toString());
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
       ans[0] = y;
       ans[1] = m;
       ans[2] = d;
       return ans;
    }

    public static boolean isLeapYear(int year) {
        return ((year % 4 == 0) && (year % 100!= 0)) || (year%400 == 0);
    }
}
