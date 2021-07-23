package helpers;

import utils.DBUtils;
import utils.FileUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class eScorePremiumId {
    public static void eScorePremiumIdGenerator() throws SQLException, IOException {
        String filePath = "/Users/piyushbansal/Downloads/ActiveLeads.csv";
        int batchSize = 2000;
        try {
            BufferedReader br = FileUtils.readFile(filePath);
            String line;
            int headerFlag = 0;
            String dbURL = DBUtils.getConnectionString();
            dbURL = dbURL.replaceAll("!", "byjus_sf_archival");
            Connection conn = DBUtils.getConnection(dbURL);

            String sql = "Select PremiumID, EngagementScore,EngagementDate,LastModifiedOn from Engagement_Score where " +
                    "EngagementDate >= NOW()- INTERVAL 90 DAY and PremiumId in (:premiumId) ";
            StringBuilder destFile = new StringBuilder("/Users/piyushbansal/Documents/Active_leads_eScore#.csv");
            String logFile = "/Users/piyushbansal/Documents/Active_leads_eScore_log.txt";
            StringBuilder premiumIdList = new StringBuilder();
            Map<String, Map<String, String>> eScorePrId = new HashMap<>();
            Map<String, String> lastModifiedMap = new HashMap<>();
            StringBuilder output=new StringBuilder();
            int curr = 0;
            int i = 0, headerSetter = 0;
            Integer counter=0,fileNumber=0;
            destFile.replace(49,50,fileNumber.toString());
//            destFile=destFile.replaceAll("#",fileNumber.toString());
            while ((line = br.readLine()) != null) {
                if (headerFlag == 0) {
                    headerFlag ^= 1;
                    output.append("PREMIUM_ID").append(",").append("LAST_MODIFIED_TIME").append(",").append("ESCORE")
                            .append(",").append("ENGAGEMENT_DATE").append("\n");
                }
                String[] listOfTokens = line.split(",");
                String tempPremiumId = listOfTokens[1].replaceAll("\"", "");
                if (tempPremiumId.isEmpty()) continue;
                else {
                    curr++;
                    counter++;//to store number of premiumId's processed till now.
                    premiumIdList.append("'").append(tempPremiumId).append("'").append(",");
                }

                if (curr == batchSize) {

                    String tmpSql = sql.replaceAll(":premiumId", premiumIdList.toString().substring(0, premiumIdList.length() - 1));
                    ResultSet rs = DBUtils.executeQuery(tmpSql, conn);

                    while (rs.next()) {
                        String currPrId = rs.getString(1);
                        String eScore = rs.getString(2);
                        String engagementDate = rs.getString(3);
                        String lastModified = rs.getString(4).substring(0, 10);

                        if (lastModifiedMap.containsKey(currPrId)) {

                        } else lastModifiedMap.put(currPrId, lastModified);

                        Map<String, String> temp;
                        if (eScorePrId.containsKey(currPrId)) temp = eScorePrId.get(currPrId);
                        else {
                            temp = new HashMap<>();
                        }
//                        System.out.println(currPrId+" "+engagementDate+" "+eScore);
                        temp.put(engagementDate, eScore);
                        eScorePrId.put(currPrId, temp);

                    }
                    rs.close();
                    //creating Output String to bee appended in file;
                    for (Map.Entry<String, Map<String, String>> itr : eScorePrId.entrySet()
                    ) {
                        String currPrId = itr.getKey();
                        Map<String, String> eScoreMap = eScorePrId.get(currPrId);
                        LocalDate date = LocalDate.now();
                        for (int j = 1; j <= 90; j++) {
                            String es;
                            if (eScoreMap.containsKey(date.toString())) {
                                es = eScoreMap.get(date.toString());
                                output.append(currPrId).append(",").append(lastModifiedMap.get(currPrId))
                                        .append(",").append(es).append(",").append(date.toString()).append("\n");
                            }
                            date = date.minusDays(1);
                        }

                    }
                    //records amended to file
                    FileUtils.writeFile(destFile.toString(), output.toString());
//                    System.out.println("Batch Executed No." + i);
                    i++;
                    conn.close();
                    //All datasets cleared to avoid memory usage.
                    conn = DBUtils.getConnection(dbURL);
                    curr = 0;
                    eScorePrId.clear();
                    lastModifiedMap.clear();
                    premiumIdList=new StringBuilder();


                }
                if(counter>=100000){
                    output=new StringBuilder();
                    premiumIdList=new StringBuilder();
                    eScorePrId.clear();
                    lastModifiedMap.clear();
                    System.out.println(counter +" Premium Id Processed in File number "+fileNumber);
                    System.out.println(destFile);
                    FileUtils.closeFile();
                    fileNumber++;
                    destFile.replace(49,50,fileNumber.toString());
                    counter=0;
                }
            }
            br.close();
            if (curr != 0) {
                String tmpSql = sql.replaceAll(":premiumId", premiumIdList.toString().substring(0, premiumIdList.length() - 1));
                ResultSet rs = DBUtils.executeQuery(tmpSql, conn);

                while (rs.next()) {
                    String currPrId = rs.getString(1);
                    String eScore = rs.getString(2);
                    String engagementDate = rs.getString(3);
                    String lastModified = rs.getString(4).substring(0, 10);

                    if (lastModifiedMap.containsKey(currPrId)) {

                    } else lastModifiedMap.put(currPrId, lastModified);

                    Map<String, String> temp;
                    if (eScorePrId.containsKey(currPrId)) temp = eScorePrId.get(currPrId);
                    else {
                        temp = new HashMap<>();
                    }
//                        System.out.println(currPrId+" "+engagementDate+" "+eScore);
                    temp.put(engagementDate, eScore);
                    eScorePrId.put(currPrId, temp);

                }
                rs.close();
                //creating Output String to bee appended in file;
                for (Map.Entry<String, Map<String, String>> itr : eScorePrId.entrySet()
                ) {
                    String currPrId = itr.getKey();
                    Map<String, String> eScoreMap = eScorePrId.get(currPrId);
                    LocalDate date = LocalDate.now();
                    for (int j = 1; j <= 90; j++) {
                        String es;
                        if (eScoreMap.containsKey(date.toString())) {
                            es = eScoreMap.get(date.toString());
                            output.append(currPrId).append(",").append(lastModifiedMap.get(currPrId))
                                    .append(",").append(es).append(",").append(date.toString()).append("\n");
                        }
                        date = date.minusDays(1);
                    }

                }
                //records amended to file
                FileUtils.writeFile(destFile.toString(), output.toString());
                System.out.println("Batch Executed No." + i);
                i++;
                conn.close();
                //All datasets cleared to avoid memory usage.
                conn = DBUtils.getConnection(dbURL);
                curr = 0;
                eScorePrId.clear();
                lastModifiedMap.clear();
                premiumIdList=new StringBuilder();

            }

        } catch (IOException e) {

            System.out.println("File not found" + e.toString());
        } finally {

        }


    }

    public static Map<String, String> createMap() {
        Map<String, String> dateMap = new HashMap<>();
        LocalDate date = LocalDate.now();
        for (int i = 1; i <= 90; i++) {
            dateMap.put(date.toString(), "");
            System.out.println(date.toString());
            date = date.minusDays(1);
        }
        return dateMap;
    }

}
