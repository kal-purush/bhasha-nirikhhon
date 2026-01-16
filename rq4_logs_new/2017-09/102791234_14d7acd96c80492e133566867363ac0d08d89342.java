/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package derby;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author dbainott
 */
public class Derby {

    Connection conn = null;
    String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    String dbName = "C:\\dbmnc";
    String dbParam = "create=true"; //la base de datos se creará si no existe todavía
    String connectionURL = "jdbc:derby:" + dbName + ";" + dbParam;
    private Thread hilo;

    public Derby() {
    this.generaBase();
        this.generaTabla();
        new Monitoreo().verificaLogin();
    }

    public static void main(String[] args) {
        new Derby();

    }

    public void  ejecutaSelect() {

        try {
            Class.forName(driver);

            conn = DriverManager.getConnection(connectionURL);

            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM users");
            while (rs.next()) {
                int idUser = rs.getInt("idUser");
                String first = rs.getString("FirstName");
                String last = rs.getString("LastName");
                System.out.println(idUser + " se llama: " + first + " " + last);
            }
            rs.close();
        } catch (Throwable e) {
            System.out.println("Ha fallado la consulta de datos");
            e.printStackTrace();
        }
    }

    public void generaTabla() {
        try {

            Statement st = conn.createStatement();
            st.executeUpdate("INSERT INTO users VALUES('Jose', 'Olmedo', 1)");
            st.executeUpdate("INSERT INTO users VALUES('Maria', 'Hoz', 2)");

            System.out.println("Se han insertado dos registros");
        } catch (Throwable e) {
            System.out.println("Ha fallado la insercion de dos registros");
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (Throwable t) {
            }
        }
    }

    public void generaBase() {

        try {
            Class.forName(driver);
        } catch (java.lang.ClassNotFoundException e) {
            e.printStackTrace();
        }

        try {
            conn = DriverManager.getConnection(connectionURL);

            Statement st = conn.createStatement();
            String sqlCreateTableUsers
                    = "CREATE TABLE users ( "
                    + "FirstName VARCHAR(20) NOT NULL, "
                    + "LastName VARCHAR(20) NOT NULL, "
                    + "idUser INTEGER NOT NULL CONSTRAINT idUser_PK PRIMARY KEY "
                    + ")";
            // la sentencia SQL crea una tabla con 3 campos
            st.execute(sqlCreateTableUsers);

            System.out.println("La base de datos '" + dbName + "' se ha creado correctamente");

        } catch (Throwable e) {
            System.out.println("Error al crear la base de datos '" + dbName + "'");
            e.printStackTrace();
        }
    }

    class Monitoreo implements Runnable {

        public void verificaLogin() {
            hilo = new Thread(this);
            hilo.start();
        }

        public void pausarHilo() {
            hilo.suspend();
        }

        public void continuarHilo() {
            hilo.resume();
        }

        public void finalizarHilo() {
            hilo.stop();
        }

        public void run() {
            while (true) {
                try {
                    new Derby().ejecutaSelect();
                    hilo.sleep(2000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(Derby.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }
}