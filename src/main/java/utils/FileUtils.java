package utils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class FileUtils {
//    static BufferedWriter bw;
    static BufferedReader br;
    public static void writeFile(String filePath, String content) throws IOException {
        File file=new File(filePath);
        file.getParentFile().mkdirs();

        BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
        bw.write(content);
        bw.close();
    }
    public static void closeFile() throws IOException {
        System.out.println("File Closed Successfully");
    }
    public static BufferedReader readFile(String filePath) throws IOException{
        File file=new File(filePath);
        br=new BufferedReader(new FileReader(file));

        return br;
    }
    public static Map<Integer,Integer> calendar(){
        Map<Integer, Integer> days = new HashMap<>();
        days.put(1, 31);
        days.put(2, 28);
        days.put(3, 31);
        days.put(4, 30);
        days.put(5, 31);
        days.put(6, 30);
        days.put(7, 31);
        days.put(8, 31);
        days.put(9, 30);
        days.put(10, 31);
        days.put(11, 30);
        days.put(12, 31);
        return days;
    }
}
