package io.github.mat3e;

import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;

class HibernateUtil {

    private static final SessionFactory sessionFactory = buildSessionfactory();

    static void close(){
        if ( sessionFactory != null ) {
            sessionFactory.close();
        }
    }

    static SessionFactory getSessionFactory(){
        return sessionFactory;
    }

    private static SessionFactory buildSessionfactory(){
        // A SessionFactory is set up once for an application!
        final StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
                .configure() // configures settings from hibernate.cfg.xml
                .build();
        try {
            return new MetadataSources( registry ).buildMetadata().buildSessionFactory();
        }
        catch (Exception e) {
            // The registry would be destroyed by the SessionFactory, but we had trouble building the SessionFactory
            // so destroy it manually.
            StandardServiceRegistryBuilder.destroy( registry );
            throw e;
        }
    }



    private HibernateUtil(){

    }
}