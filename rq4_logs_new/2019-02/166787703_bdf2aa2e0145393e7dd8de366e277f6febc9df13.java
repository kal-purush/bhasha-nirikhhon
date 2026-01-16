package kcl.paramount.group.business;

import kcl.paramount.group.dao.UserDao;
import kcl.paramount.group.dao.UserDaoJDBCImpl;
import sun.security.provider.MD5;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class UserBusiness {

    public Boolean userLogin(String username, String password) {
        Boolean flag = false;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] array = md.digest(password.getBytes("UTF-8"));
            StringBuilder sb = new StringBuilder();
            for (byte item : array) {
                sb.append(Integer.toHexString((item & 0xFF) | 0x100).substring(1, 3));
            }
            String hashed = sb.toString();

            UserDao userDao = new UserDaoJDBCImpl();
            flag = userDao.login(username, hashed);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return flag;
    }


}