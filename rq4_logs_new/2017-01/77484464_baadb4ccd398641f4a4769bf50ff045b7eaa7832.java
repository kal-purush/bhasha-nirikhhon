import java.io.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.util.*;

public class anagram extends HttpServlet
{
	protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html;charset=UTF-8");
     
		int c = 0;

    public void input()throws Exception
    {
        Scanner sc = new Scanner(System.in);
        System.out.print("Enter a word : ");
        String s = sc.next();
        System.out.println("The Anagrams are : ");
        display("",s);
        System.out.println("Total Number of Anagrams = "+c);
    }

    public void display(String s1, String s2)
    {
        if(s2.length()<=1)
        {
            c++;
            System.out.println(s1+s2);
        }
        else
        {
            for(int i=0; i<s2.length(); i++)
            {
                String x = s2.substring(i, i+1);
                String y = s2.substring(0, i);
                String z = s2.substring(i+1);
                display(s1+x, y+z);
            }
        }
    }

    public static void main(String args[])throws Exception
    {
        anagram ob=new anagram();
        ob.input();
    }
        
    }

   
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

  
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

 
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}