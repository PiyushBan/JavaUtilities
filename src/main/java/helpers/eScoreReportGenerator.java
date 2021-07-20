package helpers;

import utils.DBUtils;
import utils.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class eScoreReportGenerator {

    public static void eScoreReporter() throws Exception {

        String dbUrl = "jdbc:mysql://35.154.206.60:3306/byjus_sf_archival?user=root&password=Byjus@2020";
        String destFile = "/Users/shivendrasingh/Documents/Archival/EscoreReport/june/results.csv";
        String noPIDFile = "/Users/shivendrasingh/Documents/Archival/EscoreReport/june/results_missingPId.csv";
        String sourceFilePath = "/Users/shivendrasingh/Documents/Archival/EscoreReport/may/june.csv";

        String sql = "select a.PremiumId , a.AppId , b.aprilScore, c.mayScore, d.juneScore from ( " +
                "(select PremiumId, AppId from Engagement_Score where PremiumID in (:PremiumIDList) group by PremiumId, AppId) a " +
                "left join " +
                "(select PremiumId, AppId, sum(EngagementScore) as aprilScore from Engagement_Score where PremiumID in (:PremiumIDList) " +
                "and EngagementDate >= '2021-04-01' and EngagementDate <= '2021-04-30 group by PremiumId, AppId) b " +
                "on a.PremiumId = b.PremiumId and a.AppId = b.AppId " +
                "left join " +
                "(select PremiumId, AppId, sum(EngagementScore) as mayScore from Engagement_Score where PremiumID in (:PremiumIDList) " +
                "and EngagementDate >= '2021-05-01' and EngagementDate <= '2021-05-31' group by PremiumId, AppId) c " +
                "ON a.PremiumId = c.PremiumId and a.AppId = c.AppId " +
                "left join " +
                "(select PremiumId, AppId, sum(EngagementScore) as juneScore from Engagement_Score where PremiumID in (:PremiumIDList) " +
                "and EngagementDate >= '2021-06-01' and EngagementDate <= '2021-06-30' group by PremiumId, AppId) d " +
                "ON a.PremiumId = d.PremiumId and a.AppId = d.AppId " +
                ")";

        int batchSize = 500, curr = 0, batchNum = 0, dbConnReset = 50, temp = 0;
        StringBuilder batchIds = new StringBuilder();

        File file = new File(sourceFilePath);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        StringBuilder output = new StringBuilder();
        StringBuilder missingPidOut = new StringBuilder();
        Connection conn = DBUtils.getConnection(dbUrl);
        Map<String, String> content = new HashMap<>();

        while (((line = br.readLine()) != null)) {

            if (temp == dbConnReset) {
                FileUtils.writeFile(destFile, output.toString());
                output = new StringBuilder();
                conn.close();
                conn = DBUtils.getConnection(dbUrl);
                temp = 0;
                System.out.println("DB Connection reset done");
            }

            String id = line.split(",")[3];
            if (id.equals("") || !isNum(id)) {
                missingPidOut.append(line).append("\n");
                continue;
            } else {
                batchIds.append(id).append(",");
                content.put(id, line);
                curr++;
            }

            if (curr == batchSize) {

                batchIds = new StringBuilder(batchIds.substring(0, batchIds.length() - 1));
                String tempSQL = sql;
                tempSQL = tempSQL.replaceAll(":PremiumIDList", batchIds.toString());
                batchIds = new StringBuilder();
                System.out.println("executing query : " + tempSQL);
                ResultSet resultSet = DBUtils.executeQuery(tempSQL, conn);

                while (resultSet.next()) {
                    String premiumId = resultSet.getString("PremiumId");
                    String appId = resultSet.getString("AppId");
                    String aprilScore = resultSet.getString("aprilScore");
                    String mayScore = resultSet.getString("mayScore");
                    String juneScore = resultSet.getString("juneScore");
                    String data = content.get(premiumId);
                    output.append(data).append(",").append(appId).append(",").append(aprilScore).append(",")
                            .append(mayScore).append(",").append(juneScore).append("\n");
                }
                content.clear();
                resultSet.close();
                curr = 0;
                System.out.println("batch : " + batchNum + " executed!!");
                batchNum++;
                temp++;
            }
        }

        if (curr != 0) {

            System.out.println("last query execution begins !!!");
            batchIds = new StringBuilder(batchIds.substring(0, batchIds.length() - 1));
            String tempSQL = sql;
            tempSQL = tempSQL.replaceAll(":PremiumIDList", batchIds.toString());
            System.out.println("executing query : " + tempSQL);
            ResultSet resultSet = DBUtils.executeQuery(tempSQL, conn);

            while (resultSet.next()) {
                String premiumId = resultSet.getString("PremiumId");
                String appId = resultSet.getString("AppId");
                String aprilScore = resultSet.getString("aprilScore");
                String mayScore = resultSet.getString("mayScore");
                String juneScore = resultSet.getString("juneScore");
                String data = content.get(premiumId);
                output.append(data).append(",").append(appId).append(",").append(aprilScore).append(",")
                        .append(mayScore).append(",").append(juneScore).append("\n");
            }
            content.clear();
            resultSet.close();

            FileUtils.writeFile(destFile, output.toString());
            System.out.println("batch : " + batchNum + " executed!!");
        }

        System.out.println("all batches executed");
        FileUtils.writeFile(noPIDFile, missingPidOut.toString());
    }

    public static boolean isNum(String id) {
        try {
            Integer.parseInt(id);
        } catch(NumberFormatException | NullPointerException e) {
            return false;
        }
        return true;
    }
}
