package testcodes;
import java.io.*;

public class PythonCaller {
    public static void main(String[] args) {
        try {
            System.out.println("start");
            File directory = new File("");
            String courseFile = directory.getCanonicalPath() ;
            String temp2 = "src\\main\\webapp\\python\\callee.py";
            String[] args1=new String[]{"python",temp2,"--arg","3"};
            Process pr=Runtime.getRuntime().exec(args1);

            BufferedReader in = new BufferedReader(new InputStreamReader(
            pr.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                System.out.println(line);
            }
            in.close();
            pr.waitFor();
            System.out.println("end");
        } catch (Exception e) {
            e.printStackTrace();
        }}
    public void test(){
        System.out.println("我的第一个方法C");
    }
}