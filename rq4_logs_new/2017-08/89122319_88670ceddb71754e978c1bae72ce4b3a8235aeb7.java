    import java.io.*;  
    import java.*;  
    import javax.servlet.ServletException;  
    import javax.servlet.http.*;  
      
    public class register extends HttpServlet {  
    public void doPost(HttpServletRequest request, HttpServletResponse response)  
                throws ServletException, IOException {  
      
    response.setContentType("text/html");  
    PrintWriter out = response.getWriter();  
              
    
    String un=request.getParameter("uname");
    String p=request.getParameter("pwd");
              
    try{  
   
    	if(un.equals("abc")&&p.equals("123"))
    	{
    		out.println("logged in successfull");
    	}
    	else
    	{
    		out.println("incorrect password or username");
    	}
    	
    }catch (Exception e2) {System.out.println(e2);}  
              
    out.close();  
    }  
      
    }  