package my.example.miniboard.servlet;

import my.example.miniboard.dao.ChampDAO;
import my.example.miniboard.util.JdbcUtil;
import my.example.miniboard.vo.Champ;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Enumeration;
import java.util.Map;

@WebServlet("/addChamp")
public class ChampAddServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        RequestDispatcher rd = req.getRequestDispatcher("/WEB-INF/views/board/addChamp.jsp");
        rd.forward(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        req.setCharacterEncoding("utf-8");

        // 로그인 확인

        // admin 계정인지 확인

        ChampDAO dao = new ChampDAO();
        Champ champ = new Champ();

        champ.setCname(req.getParameter("champName"));
        champ.setRole(req.getParameter("role"));
        champ.setHp1(req.getParameter("hp1"));
        champ.setHp18(req.getParameter("hp18"));
        champ.setMp1(req.getParameter("mp1"));
        champ.setMp18(req.getParameter("mp18"));
        champ.setAtk1(req.getParameter("atk1"));
        champ.setAtk18(req.getParameter("atk18"));
        champ.setDf1(req.getParameter("df1"));
        champ.setDf18(req.getParameter("df18"));
        champ.setRange1(req.getParameter("ar1"));
        champ.setRange18(req.getParameter("ar18"));
        champ.setPassive(req.getParameter("passive"));
        champ.setPassive_comment(req.getParameter("passiveComment"));
        champ.setSkillq(req.getParameter("skillQ"));
        champ.setSkillq_comment(req.getParameter("skillQComment"));
        champ.setSkillw(req.getParameter("skillW"));
        champ.setSkillw_comment(req.getParameter("skillWComment"));
        champ.setSkille(req.getParameter("skillE"));
        champ.setSkille_comment(req.getParameter("skillEComment"));
        champ.setSkillr(req.getParameter("skillR"));
        champ.setSkillr_comment(req.getParameter("skillRComment"));
        dao.addChamp(req);

        resp.sendRedirect("/champList");
    }
}