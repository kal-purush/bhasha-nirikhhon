package ru.sstu.se20.common;

import org.hibernate.Session;
import ru.sstu.se20.DAL.DAO.UsersDAO;
import ru.sstu.se20.DAL.Entities.UsersEntity;
import ru.sstu.se20.utils.HibernateUtil;

import java.util.Date;

public class ControllerUtils {


    public static UsersEntity getUser(String cookie) {
        UsersDAO dao = new UsersDAO();
        UsersEntity user = dao.getByCookie(cookie);
        if (user == null) {
            user = new UsersEntity();
            Session sessions = HibernateUtil.getSessionFactory().openSession();
            sessions.beginTransaction();
            Date cookieDate = new Date();
            user.setCookie(cookie);
            user.setCookieCreationDate(cookieDate);
            sessions.save(user);
            sessions.getTransaction().commit();
            sessions.close();
        }
        return user;
    }
}