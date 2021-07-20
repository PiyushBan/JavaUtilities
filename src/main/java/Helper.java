import java.util.Scanner;

import helpers.*;

public class Helper {

    public static void main(String[] args) throws Exception {
        String[] DbName = new String[10];
        DbName[1] = "byjus_sf_archival";
        DbName[2] = "byjus_sf_archival_dev";
        DbName[3] = "ls_11005";
        DbName[4] = "ls_31593";
        DbName[5] = "ls_7461";
        DbName[6]="Migration for Engagement Score Direct";
        Scanner scanner = new Scanner(System.in);
        for (int i = 1; i < 7; i++) {
            System.out.println(i + ": " + DbName[i]);
        }
        int n = scanner.nextInt();
        switch (n) {
            case 1:
                DbTableSelector.Archival(DbName[1]);
                break;
            case 2:
                DbTableSelector.Archival_dev(DbName[2]);
                break;
            case 3:
                DbTableSelector.ls_11005(DbName[3]);
                break;
            case 4:
                DbTableSelector.ls_31593(DbName[4]);
                break;
            case 5:
                DbTableSelector.ls_7461(DbName[5]);
                break;
            case 6:
                helpers.migrationHelper.eScoreMigrator();
                break;

            default:
                System.out.println("Wrong DB selected. Exiting...");

        }

    }
}

