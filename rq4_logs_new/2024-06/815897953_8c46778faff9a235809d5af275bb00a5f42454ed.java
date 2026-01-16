import java.io.*;
public class Count_words_para
{
    public static void main(String []args)
    {
        try
        {
            int cnt=1;
            FileReader f = new FileReader("D:\\text.txt");
            int b;
            String str="";
            while((b=f.read())!=-1)
            {
                char c=(char)b;
                str+=c;
                
            }
            System.out.println(str);
            
            if(str!=null)
            {
                for(int i=0;i<str.length();i++)
                {
                    if((str.charAt(i)==' ') && (str.charAt(i+1)!=' '))
                    {
                       cnt++;
                    }
                }
            }
            System.out.println("The number of words present in the paragraph are  "+cnt);
        } 
        catch(IOException e)
        {
            System.out.println("handled");
        }
    }
}