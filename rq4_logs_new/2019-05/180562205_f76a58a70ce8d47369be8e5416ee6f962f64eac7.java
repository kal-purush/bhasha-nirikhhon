package SAD.Servlets;

import SAD.Database.DataOperation;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;

public class Follow extends HttpServlet {
    @Override
    @SuppressWarnings("unchecked")
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        HttpSession session=req.getSession();
        req.setCharacterEncoding("UTF-8");
        resp.setCharacterEncoding("UTF-8");
        resp.setContentType("text/html;charset=utf-8");

        String userName = req.getParameter("userName");
        String followName = req.getParameter("expertName");
        ApplicationContext context=new ClassPathXmlApplicationContext("spring_config.xml");
        DataOperation dataoperator = (DataOperation) context.getBean("dataoperator");
        int id = dataoperator.getUserId(userName);
        int followId = dataoperator.getUserId(followName);
        int followState =  dataoperator.followState(id,followId);
        if (followState==0) {
            dataoperator.followUser(id,followId);
        } else {
            dataoperator.unFollowUser(id,followId);
        }

        String isFollowing = (followState==0) ? "取消关注" : "关注" ;

        try {

            req.getRequestDispatcher("/expertHome.jsp?name="+userName+"&isFollowing="+isFollowing).forward(req, resp);


        } catch(Exception e){
            // resp.sendRedirect(req.getContextPath() + "/htmls/index.html");
        }
    }
}