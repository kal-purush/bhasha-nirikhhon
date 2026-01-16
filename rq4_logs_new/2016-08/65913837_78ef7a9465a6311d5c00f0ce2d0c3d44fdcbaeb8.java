package main.java.connection;

import main.java.entities.User;
import org.apache.commons.dbcp2.BasicDataSource;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.hibernate5.HibernateTransactionManager;
import org.springframework.orm.hibernate5.LocalSessionFactoryBuilder;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Configuration
public class HibernateConfiguration
{
    @Autowired
    @Qualifier("hibernateProps")
    private Properties connectionProps;

    @Bean(name = "dataSource")
    public DataSource getDataSource() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(connectionProps.getProperty("jdbc.driverClassName"));
        dataSource.setUrl(connectionProps.getProperty("jdbc.url"));
        dataSource.setUsername(connectionProps.getProperty("jdbc.username"));
        dataSource.setPassword(connectionProps.getProperty("jdbc.password"));
        return dataSource;
    }

    @Autowired
    @Bean(name = "sessionFactory")
    public SessionFactory getSessionFactory(DataSource dataSource) {

        LocalSessionFactoryBuilder sessionBuilder = new LocalSessionFactoryBuilder(dataSource);

        sessionBuilder.addAnnotatedClasses(User.class);

        sessionBuilder.addProperties(connectionProps);

        return sessionBuilder.buildSessionFactory();
    }


    @Autowired
    @Bean(name = "transactionManager")
    public HibernateTransactionManager getTransactionManager(
            SessionFactory sessionFactory) {
        HibernateTransactionManager transactionManager = new HibernateTransactionManager(
                sessionFactory);

        return transactionManager;
    }
    @Bean(name = "hibernateProps")
    public Properties getConnectionProps() {
        Properties props = new Properties();
        InputStream input = null;

        String dir = System.getProperty("user.dir");
        System.out.println("current dir = " + dir);
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            input = classLoader.getResourceAsStream("main/resources/hibernate.properties");
            // load a properties file
            props.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return props;
    }

}