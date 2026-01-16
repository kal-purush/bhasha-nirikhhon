package ru.clevertec.carshowroom.util;

import lombok.Getter;
import lombok.experimental.UtilityClass;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import ru.clevertec.carshowroom.entity.CarEntity;
import ru.clevertec.carshowroom.entity.CarShowroomEntity;
import ru.clevertec.carshowroom.entity.CategoryEntity;
import ru.clevertec.carshowroom.entity.ClientEntity;
import ru.clevertec.carshowroom.entity.ReviewEntity;

@UtilityClass
public class HibernateUtil {

    @Getter
    private static final SessionFactory sessionFactory;

    static {
        try {
            sessionFactory = new Configuration().configure("hibernate.cfg.xml")
                    .addAnnotatedClass(CarEntity.class)
                    .addAnnotatedClass(CarShowroomEntity.class)
                    .addAnnotatedClass(ClientEntity.class)
                    .addAnnotatedClass(CategoryEntity.class)
                    .addAnnotatedClass(ReviewEntity.class)
                    .buildSessionFactory();
        } catch (Exception ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    public static Session getSession() {
        return sessionFactory.openSession();
    }

    public static void shutdown() {
        getSessionFactory().close();
    }
}