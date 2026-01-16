package jumble1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class Jumble1 {
    
    public static ArrayList<String> words = new ArrayList();
    
    public static int unjumble(String word, String fin)
    {         
        for (int i = 0; i < word.length(); i++)
        {
            if (word.length() == 1)
            {
                fin += word;
                words.add(fin);
                //System.out.println(fin);
                return 0;
            }
                  
            String temp = word.substring(i, i+1);
            
            String temp1 = "";
            
            for (int j = 0; j < word.length(); j++)
            {
                if (j != i)
                {
                    temp1 += word.substring(j, j+1);
                }
            }
           
            unjumble(temp1, fin+temp);
        }
        return 0;
    }
    
    public static void main(String[] args) throws Exception
    {
        String s = "wearly";
        String fin = "";
        unjumble(s, fin);
        
        for (String str : words)
        {
            System.out.println(str);
        }
        
        BufferedReader CSVFile = new BufferedReader(new FileReader(
                "C:\\Users\\Masilan\\Documents\\NetBeansProjects\\EOWL-v1.1.2\\EOWL-v1.1.2\\CSV Format\\A Words"));
        
    }
    
}
