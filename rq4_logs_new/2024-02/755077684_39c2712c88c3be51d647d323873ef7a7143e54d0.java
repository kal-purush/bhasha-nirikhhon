import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Random;

public class MotivationBible {
    public static void main(String[] args)throws IOException, InterruptedException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        Random random = new Random();
        int choices;
        String[] Gospel = {"Psalm 23:4 Even though I walk through I walk "};
        do{
            choices = Integer.parseInt(br.readLine());
            switch (choices) {
                case 1:
                    
                    break;
                case 2:
                break;
                default:
                    break;
            }
        }while(choices !=2);
    }
}