package Inleveropracht1;


import java.text.ParseException;

public class Main {


    public static void main(String[] args) throws ParseException {
        ReizigerDAOImpl DAO = new ReizigerDAOImpl();
        Reiziger R1 = new Reiziger(0, "A", null, "Ruizendaal",  java.sql.Date.valueOf("1990-12-23"));
        Reiziger R2 = new Reiziger(1, "J", null, "Ruizendaal",  java.sql.Date.valueOf("1968-10-13"));
        Reiziger R3 = new Reiziger(1, "T", null, "Ruizendaal",  java.sql.Date.valueOf("1956-9-13"));
        DAO.save(R1);
        DAO.save(R2);
        DAO.save(R3);
        System.out.println(DAO.findAll());
        R1.setGbdatum( java.sql.Date.valueOf("1999-04-23"));
        DAO.update(R1);
        DAO.delete(R2);
        System.out.println(DAO.findAll());
        System.out.println(DAO.findByGBdatum("1999-04-23"));
        System.out.println(DAO.findByGBdatum("1999-05-23"));
    }
}