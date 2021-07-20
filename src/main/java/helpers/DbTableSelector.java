package helpers;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Scanner;

public class DbTableSelector {
    static Scanner sc = new Scanner(System.in);

    public static void Archival(String dbName) throws SQLException,IOException {
        String[] TableName = new String[]{"",
                "11005_ActivityTypes",
                "11005_users",
                "31593_ActivityTypes",
                "31593_users",
                "35742_users",
                "37542_ActivityTypes",
                "7461_ActivityTypes",
                "7461_activities_to_be_exposed_in_salesforce",
                "7461_users",
                "Engagement_Score"
        };
        for (int i = 1; i < TableName.length; i++) {
            System.out.println(i + ": " + TableName[i]);
        }
        int n = sc.nextInt();
        if (n >= TableName.length) {
            System.out.println("Wrong Choice Entered, Exiting...");
            return;
        }
        if(n==10){
            TableMigrator.specialMigrator(dbName,TableName[n]);
        }
        migratorhelper(dbName, TableName[n]);

    }

    public static void Archival_dev(String dbName) throws SQLException,IOException{

    }

    public static void ls_11005(String dbName) throws SQLException,IOException{
        String[] TableName = new String[]{"",
               "ProspectNote_Base"
        };
        for (int i = 1; i < TableName.length; i++) {
            System.out.println(i + ": " + TableName[i]);
        }
        int n = sc.nextInt();
        if (n >= TableName.length) {
            System.out.println("Wrong Choice Entered, Exiting...");
            return;
        }
        migratorhelper(dbName, TableName[n]);

    }

    public static void ls_31593(String dbName) throws SQLException,IOException{
        String[] TableName = new String[]{"",
                "ProspectActivity_Base",
                "ProspectActivity_ExtensionBase",
                "ProspectNote_Base",
                "Prospect_Base",
                "Prospect_ExtensionBase",
        };
        for (int i = 1; i < TableName.length; i++) {
            System.out.println(i + ": " + TableName[i]);
        }
        int n = sc.nextInt();
        if (n >= TableName.length) {
            System.out.println("Wrong Choice Entered, Exiting...");
            return;
        }
        migratorhelper(dbName, TableName[n]);

    }

    public static void ls_7461(String dbName) throws SQLException,IOException{
        String[] TableName = new String[]{"",
                "ProspectActivity_Base",
                "ProspectActivity_Base_494",
                "ProspectActivity_ExtensionBase",
                "ProspectNote_Base",
                "Prospect_Base",
                "Prospect_ExtensionBase",
                "Users"
        };
        for (int i = 1; i < TableName.length; i++) {
            System.out.println(i + ": " + TableName[i]);
        }
        int n = sc.nextInt();
        if (n >= TableName.length) {
            System.out.println("Wrong Choice Entered, Exiting...");
            return;
        }
        migratorhelper(dbName, TableName[n]);

    }

    public static void migratorhelper(String dbName, String tableName) throws SQLException, IOException {
        TableMigrator.migrator(dbName, tableName);
    }
}
