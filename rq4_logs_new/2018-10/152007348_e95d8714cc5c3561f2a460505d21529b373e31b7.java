package my.example.miniboard.dao;

import my.example.miniboard.util.JdbcUtil;

import javax.servlet.http.HttpServletRequest;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class ChampDAO {

    public int addChamp(HttpServletRequest req){

        Connection conn = null;
        PreparedStatement ps = null;
        int result = 0;

        String sql = "INSERT INTO champboard (cname, role, hp1, hp18, mp1, mp18, atk1, atk18, df1, df18, range1, range18, passive, passive_comment, skillq, skillq_comment, skillw, skillw_comment, skille, skille_comment, skillr, skillr_comment, register_date) " +
                "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,now())";

        try {
            conn = JdbcUtil.connection();
            ps = conn.prepareStatement(sql);
            ps.setString(1,req.getParameter("champName"));
            ps.setString(2,req.getParameter("role"));
            ps.setString(3,req.getParameter("hp1"));
            ps.setString(4,req.getParameter("hp18"));
            ps.setString(5,req.getParameter("mp1"));
            ps.setString(6,req.getParameter("mp18"));
            ps.setString(7,req.getParameter("atk1"));
            ps.setString(8,req.getParameter("atk18"));
            ps.setString(9,req.getParameter("df1"));
            ps.setString(10,req.getParameter("df18"));
            ps.setString(11,req.getParameter("ar1"));
            ps.setString(12,req.getParameter("ar18"));
            ps.setString(13,req.getParameter("passive"));
            ps.setString(14,req.getParameter("passiveComment"));
            ps.setString(15,req.getParameter("skillQ"));
            ps.setString(16,req.getParameter("skillQComment"));
            ps.setString(17,req.getParameter("skillW"));
            ps.setString(18,req.getParameter("skillWComment"));
            ps.setString(19,req.getParameter("skillE"));
            ps.setString(20,req.getParameter("skillEComment"));
            ps.setString(21,req.getParameter("skillR"));
            ps.setString(22,req.getParameter("skillRComment"));
            result = ps.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(conn, ps);
        }

        return result;
    }
}