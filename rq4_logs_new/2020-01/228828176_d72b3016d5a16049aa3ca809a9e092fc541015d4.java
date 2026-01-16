package dao;

import entity.UserInfo;
import utils.DbConn;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author ：Tong
 * @date ：Created in 2020/1/28 20:37
 * @description：
 * @version: $
 */
public class UserDao {

    public UserInfo queryUserInfo(int userMobile) {

        ArrayList<Parameter> parameters = new ArrayList<>();

        parameters.add(new Parameter("and", "pui_user_mobile", "=", userMobile));

        UserInfo userInfo = null;
        try {
            Connection conn = DbConn.getConn();
            String sql = SQLString.queryUserMain;
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setInt(1, userMobile);

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                userInfo.setUserId(rs.getString("pui_id"));
                userInfo.setUserMobile(rs.getString("pui_user_mobile"));
                userInfo.setUserEmail(rs.getString("pui_user_email"));
                userInfo.setUserName(rs.getString("pui_user_name"));
                userInfo.setUserStt(rs.getInt("pui_user_stt"));
            }

            DbConn.closeConn(ps, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return userInfo;
    }

    public UserInfo queryUserInfoByEmail(String userEmail) {

        return new UserInfo();
    }

    public UserInfo queryUserInfoByUserName(String userName) {
        return new UserInfo();
    }


}