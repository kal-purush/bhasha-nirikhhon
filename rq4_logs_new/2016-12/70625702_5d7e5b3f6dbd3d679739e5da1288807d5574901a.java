package northwind.rest.app.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.hibernate.internal.SessionFactoryImpl;

import northwind.rest.app.util.HibernateUtil;

public class Common {
    private static String url, user, pass;
    private static Connection dbConn;

    public static String createTestDB(String testDbName) throws SQLException {
        // get connection string from hibernate using the session factory
        Properties props = ((SessionFactoryImpl)HibernateUtil.getSessionFactory()).getProperties();
        url  = props.get("hibernate.connection.url").toString();
        user = props.get("hibernate.connection.username").toString();
        pass = props.get("hibernate.connection.password").toString();

        // create a new temporary test database
        String updatedUrl = url.replaceAll("northrest", testDbName);
        dbConn = DriverManager.getConnection(url, user, pass);
        Statement statement = dbConn.createStatement();
        statement.executeUpdate("CREATE DATABASE " + testDbName);
        importDB(DriverManager.getConnection(updatedUrl, user, pass), "src/main/resources/schema.sql");
        return updatedUrl;
    }

    public static void dropTestDB(String testDbName) throws SQLException {
        Statement statement = dbConn.createStatement();
        statement.executeUpdate("DROP DATABASE " + testDbName);
        dbConn.close();
    }

    private static void importDB(Connection db, String fileName) {
        String s = new String();
        StringBuffer sb = new StringBuffer();

        try {
            FileReader fr = new FileReader(new File(fileName));
            BufferedReader br = new BufferedReader(fr);
 
            while((s = br.readLine()) != null) {
                // skip comments
                if (s.startsWith("--"))
                    continue;
                sb.append(s);
            }
            br.close();

            // We use ";" as a delimiter for each statement
            String[] inst = sb.toString().split(";");
            Statement st = db.createStatement();

            for(int i = 0; i<inst.length; i++) {
                // we ensure that there is no spaces before or after the request string
                // in order to not execute empty statements
                if(!inst[i].trim().equals("")) {
                    st.executeUpdate(inst[i]);
                }
            }
            // close provided connection
            db.close();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    public static Date asDate(String dateStr) {
        Date date = null;
        try {
            date = dateFormat.parse(dateStr);
        } catch (ParseException e1) {}
            
        return date;
    }

}