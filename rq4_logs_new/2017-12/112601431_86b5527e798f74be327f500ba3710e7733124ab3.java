package fr.pb.global;

import java.sql.Connection;
import javax.swing.JFrame;
import redis.clients.jedis.Jedis;

/**
 *
 * @author pascal
 */
public class Globale {

    private static JFrame fenetre;

    private static String ip;
    private static String port;
    private static String user;
    private static String mdp;
    private static Connection connexionMySQL;
    private static Jedis connexionRedis;

    public static JFrame getFenetre() {
        return fenetre;
    }

    public static void setFenetre(JFrame fenetre) {
        Globale.fenetre = fenetre;
    }

    public static String getIp() {
        return ip;
    }

    public static void setIp(String ip) {
        Globale.ip = ip;
    }

    public static String getPort() {
        return port;
    }

    public static void setPort(String port) {
        Globale.port = port;
    }

    public static String getUser() {
        return user;
    }

    public static void setUser(String user) {
        Globale.user = user;
    }

    public static String getMdp() {
        return mdp;
    }

    public static void setMdp(String mdp) {
        Globale.mdp = mdp;
    }

    public static Connection getConnexionMySQL() {
        return connexionMySQL;
    }

    public static void setConnexionMySQL(Connection connexionMySQL) {
        Globale.connexionMySQL = connexionMySQL;
    }

    public static Jedis getConnexionRedis() {
        return connexionRedis;
    }

    public static void setConnexionRedis(Jedis connexionRedis) {
        Globale.connexionRedis = connexionRedis;
    }

} /// class