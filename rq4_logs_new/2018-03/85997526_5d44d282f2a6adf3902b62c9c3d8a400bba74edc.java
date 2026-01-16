package ab4;

import java.io.*;
import java.util.*;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author marinbulachi
 */
public class Ab4 {

    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";
    private static final String URL = "jdbc:mysql://localhost/Loc";

    public static void main(String[] args) {
        Connection connect;
        Statement statement;
        String command;
        try {
            // Conectarea la server
            Class.forName("com.mysql.jdbc.Driver");
            connect = DriverManager.getConnection(URL,
                    PASSWORD, USERNAME);
            statement = connect.createStatement();
            OUTER:
            while (true) {
                System.out.println("\nCe comada doriti sa efectuati?");
                Scanner scanner = new Scanner(System.in);
                command = scanner.nextLine();
                /*Verific ce comanda a fost scrisa*/
                switch (command) {
                    case "help":
                        System.out.println("Comenzile posibele sunt: getInformation,"
                                + " getTop5, getActivity, exit, help");
                        break;
                        
                    case "exit":
                        break OUTER;
                        
                    case "getInformation":
                        System.out.println("Despre ce localitate doriti sa aflati "
                                + "informatie?");
                        scanner = new Scanner(System.in);
                        command = scanner.nextLine();
                        getInformation(statement, command);
                        break;
                        
                    case "getActivity":
                        System.out.println("Ce activitate doriti sa practicati?");
                        scanner = new Scanner(System.in);
                        command = scanner.nextLine();
                        getActivity(statement, command);
                        break;
                        
                    case "getTop5":
                        System.out.println("Alegeti tara/judetul/orasul si perioada "
                                + "in formatul YYYY-WW-DD");
                        System.out.println("Exempu: Romania 2018-10-2 2019-12-31");
                        scanner = new Scanner(System.in);
                        String[] line = scanner.nextLine().split(" ");
                        if (line.length < 3) {
                            System.out.println("Comada gresita\nNu ati introdus "
                                    + "3 parametri");
                        } else {
                            String loc = line[0];
                            String startData = line[1];
                            String endData = line[2];
                            getTop5(statement, loc, startData, endData);
                        }   break;
                    default:
                        System.out.println("Comanda gresita!\nPentru a vedea lista "
                                + "de comenzi scrieti help");
                        break;
                }
            }

        } catch (SQLException ex) {
            Logger.getLogger(Ab4.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Ab4.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    /**
     * Metoda face o cerere select la baza de date
     * le sorteaza dupa pret si intoarce doar prima linie
     * @param statement
     * @param activity 
     */
    private static void getActivity(Statement statement, String activity) {
        ResultSet resultSet = null;
        try {
            resultSet = statement.executeQuery("select * from Localitate "
                    + "where " + activity + " = 1 "
                    + "ORDER BY Pret LIMIT 1;");
        } catch (SQLException ex) {
            System.out.println("Comanda gresita!");
        }
        printResult(resultSet);
    }

    /**
     * Metoda face o cerere select la baza de date
     * Sorteaza raspunsul dupa pret afiseaza doar primele 5
     * @param statement
     * @param loc
     * @param startData
     * @param endData 
     */
    public static void getTop5(Statement statement, String loc, String startData, String endData) {
        ResultSet resultSet = null;
        try {
            resultSet = statement.executeQuery("select * from Localitate where (Tara = \""
                    + loc + "\" or Orasul = \"" + loc + "\" or Judetul = \"" + loc + "\")"
                    + " and ((\"" + startData + "\"between  Start_data and End_data)"
                    + " or (\"" + endData + "\" between Start_data and End_data))"
                    + "ORDER BY Pret LIMIT 5;");
        } catch (SQLException ex) {
            System.out.println("Comanda gresita!");
        }
        printResult(resultSet);
    }

    /**
     * Metoda face o cerere select la baza de date
     * cu conditia ca locatia sa fie egala cu locatia scrisa de client
     * @param statement
     * @param location 
     */
    public static void getInformation(Statement statement, String location) {

        ResultSet resultSet = null;
        try {
            resultSet = statement.executeQuery("select * from Localitate where"
                    + " Locatie = \"" + location + "\" ");
        } catch (SQLException ex) { 
            Logger.getLogger(Ab4.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        printResult(resultSet);

    }

    /**
     * 
     * Metoda afiseaza rezultatul;
     * @param resultSet 
     */
    public static void printResult(ResultSet resultSet) {
        int ok=0;
        try {
            if (resultSet == null) {
                return;
            }
            // cat timp exita un loc
            while (resultSet.next()) {
                ok++;
                System.out.println("-----------------------------------------");
                String tara = resultSet.getString("Tara");
                System.out.println("Tara: " + tara);

                String judetul = resultSet.getString("Judetul");
                System.out.println("Judetul: " + judetul);

                String orasul = resultSet.getString("Orasul");
                System.out.println("Orasul: " + orasul);

                String locatie = resultSet.getString("Locatie");
                System.out.println("Locatie: " + locatie);

                String pret = resultSet.getString("Pret");
                System.out.println("Pret: " + pret + " Lei");

                String startData = resultSet.getString("Start_data");
                System.out.println("Incepind cu luna: " + startData);

                String endData = resultSet.getString("End_data");
                System.out.println("Pana in luna: " + endData);

                System.out.print("Sunt posibile urmatoarele activitati : ");
                String trase_montane = resultSet.getString("Trase_montane");

                if (trase_montane.contains("1")) {
                    System.out.print("Trase montane, ");
                }

                String plaja = resultSet.getString("Plaja");
                if (plaja.contains("1")) {
                    System.out.print("Plaja, ");
                }

                String inot = resultSet.getString("Inot");
                if (inot.contains("1")) {
                    System.out.print("Inot, ");
                }

                String muzeu = resultSet.getString("Muzeu");
                if (muzeu.contains("1")) {
                    System.out.print("Muzeu, ");
                }

                String teatru = resultSet.getString("Teatru");
                if (teatru.contains("1")) {
                    System.out.print("Teatru, ");
                }

                String turistic = resultSet.getString("Turistic");
                if (turistic.contains("1")) {
                    System.out.print("Turizm");
                }
                System.out.print("\n");

            }
            if(ok == 0){
                 System.out.println("Nu am gasit nici un raspus!");
            }
        } catch (SQLException ex) {
            Logger.getLogger(Ab4.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }
}