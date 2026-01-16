package Servlet;

import JavaBean.CargoBean;
import JavaBean.IndentBean;
import JavaBean.InfoBean;
import JavaBean.UserBean;
import Util.SQLConnector;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @Author: michael
 * @Date: 16-7-22 上午8:19
 * @Project: S.M.
 * @Package: ${PACKAGE_NAME}
 */
@WebServlet(name = "AddressDelete")
public class AddressDelete extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        request.setCharacterEncoding("utf-8");
        int id = Integer.parseInt(request.getParameter("id"));
        UserBean userBean = (UserBean) request.getSession().getAttribute("userBean");
        InfoBean infoBean = (InfoBean) request.getSession().getAttribute("infoBean");
        String forward;
        try{
            SQLConnector connector = new SQLConnector();
            String sql = "DELETE FROM address WHERE id="+id;
            connector.update(sql);
            forward = "/HandleShow?type=address&sign=" + userBean.getId();
            connector.con().close();
        } catch(SQLException e) {
            infoBean.setInfo("数据库访问错误,请重试。");
            forward = "/login.jsp";
            e.printStackTrace();
        }
        RequestDispatcher requestDispatcher = request.getRequestDispatcher(forward);
        requestDispatcher.forward(request, response);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doPost(request, response);
    }
}