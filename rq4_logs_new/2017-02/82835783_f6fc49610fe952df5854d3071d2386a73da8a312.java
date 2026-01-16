import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

public class Profile extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String username = (String) req.getSession().getAttribute("username");
        if(username == null){
            resp.sendRedirect("/login");
        }
        else {
            PrintWriter print = resp.getWriter();
            print.write("<h1>" + "Hi Summoner " + username + "</h1>");
            print.write("<p><a href=\"findSum\">Find Summoner</a></p>");
            print.write("<p><a href=\"champs\">Look for builds</a></p>");
        }
    }
}