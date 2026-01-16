package my.example.miniboard.Dao;

import my.example.miniboard.vo.User;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class UserDAO {

        private static UserDAO instance = new UserDAO();

        private UserDAO(){ }
        public static UserDAO getInstance(){
            return instance;

        }

public void insertUser(User user) throws SQLException{
            Connection conn = null;
            PreparedStatement ps = null;


        }
}