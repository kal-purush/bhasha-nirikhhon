package controller;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import kic.mskim.Login;
import kic.mskim.MskimRequestMapping;
import kic.mskim.RequestMapping;

@WebServlet("/admin/*")
public class AdminController extends MskimRequestMapping {
   

   @Login(key = "id" ,value="admin")
   
   HttpSession session;
   @RequestMapping("main")
   public String main(HttpServletRequest req, HttpServletResponse res) throws Exception {
      // TODO Auto-generated method stub
      return "/WEB-INF/view/admin/main.jsp";
   }
   
   @Override
   protected void service(HttpServletRequest request, HttpServletResponse resp) throws ServletException, IOException {
      request.setCharacterEncoding("utf-8");
      this.session = request.getSession();
      super.service(request, resp);
   }
   
   @RequestMapping("boardban")
   public String boardban(HttpServletRequest req, HttpServletResponse res) throws Exception {
      
      
      
      return "/WEB-INF/view/admin/boardban.jsp";
      
      
      
      
   }
   
}