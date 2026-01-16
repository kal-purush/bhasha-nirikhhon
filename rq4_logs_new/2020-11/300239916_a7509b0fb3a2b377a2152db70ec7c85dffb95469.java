package at.dcv.w12;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class SQLImportNewCitymitObjekt {

    private Connection connect = null;

    //einfache Query
    private Statement statement = null;
    //Query mit ?
    private PreparedStatement preparedStatement = null;
    private ResultSet resultSet = null;
    private Scanner sc = new Scanner(System.in);

    public static void main(String[] args) {
        SQLImportNewCitymitObjekt psq = new SQLImportNewCitymitObjekt();
        //dbTest.readDataBase("select * from mondial.country");
        psq.readDataBase();
    }

    public void insertCity(City c) throws SQLException {

        String select = "INSERT INTO mondial.city VALUES (?, ?, ? ,?,null,null);";

        // Statements allow to issue SQL queries to the database
        preparedStatement = connect.prepareStatement(select);
        preparedStatement.setString(1, c.getCityName());
        preparedStatement.setString(2, c.getCityCountry());
        preparedStatement.setString(3, c.getCityProvince());
        preparedStatement.setInt(4, Integer.parseInt(c.getCityPopulation()));
        // Result set get the result of the SQL query
        preparedStatement.executeUpdate();
    }

    public class City {
        private String cityName;
        private String cityCountry;
        private String cityProvince;
        private String cityPopulation;

        public City(String cityName, String cityCountry, String cityProvince, String cityPopulation) {
            this.cityName = cityName;
            this.cityCountry = cityCountry;
            this.cityProvince = cityProvince;
            this.cityPopulation = cityPopulation;
        }

        public String getCityCountry() {
            return cityCountry;
        }

        public String getCityName() {
            return cityName;
        }

        public String getCityProvince() {
            return cityProvince;
        }

        public String getCityPopulation() {
            return cityPopulation;
        }
    }


    private City getCity() {
        City c = null;

        boolean eingabeOK = false;
        do {
            try {
                System.out.println("Bitte Stadt eingeben");
                String city = sc.nextLine();
                System.out.println("Bitte land eingeben");
                String country = sc.nextLine();
                System.out.println("Bitte Provinz eingeben");
                String province = sc.nextLine();
                System.out.println("Bitte Population eingeben");
                String population = sc.nextLine();

                c = new City(city, country, province, population);
                System.out.println("Daten sind zugefügt");
                eingabeOK = true;
            } catch (NumberFormatException nfe) {
                System.out.println("fehlerhafte Eingabe");
            }
        } while (eingabeOK == false);

        return c;
    }


    public void readDataBase() {
        try {
            // Setup the connection with the DB
            connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/mondial?"
                            + "serverTimezone=UTC", "root", "Omemotuwe2020#");

//                queryCity(getCity(sc));
            insertCity(getCity());

/*
            // PreparedStatements can use variables and are more efficient
            preparedStatement = connect
                    .prepareStatement("insert into  feedback.comments values (default, ?, ?, ?, ? , ?, ?)");
            // "myuser, webpage, datum, summary, COMMENTS from feedback.comments");
            // Parameters start with 1
            preparedStatement.setString(1, "Test");
            preparedStatement.setString(2, "TestEmail");
            preparedStatement.setString(3, "TestWebpage");
            preparedStatement.setDate(4, new java.sql.Date(2009, 12, 11));
            preparedStatement.setString(5, "TestSummary");
            preparedStatement.setString(6, "TestComment");
            preparedStatement.executeUpdate();

            preparedStatement = connect
                    .prepareStatement("SELECT myuser, webpage, datum, summary, COMMENTS from feedback.comments");
            resultSet = preparedStatement.executeQuery();
            writeResultSet(resultSet);

            // Remove again the insert comment
            preparedStatement = connect
                    .prepareStatement("delete from feedback.comments where myuser= ? ; ");
            preparedStatement.setString(1, "Test");
            preparedStatement.executeUpdate();

            resultSet = statement
                    .executeQuery("select * from feedback.comments");
            writeMetaData(resultSet);
*/
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        } finally {
            close();
        }

    }

    private void writeMetaData(ResultSet resultSet) throws SQLException {
        //  Now get some metadata from the database
        // Result set get the result of the SQL query

        System.out.println("The columns in the table are: ");

        System.out.println("Table: " + resultSet.getMetaData().getTableName(1));
        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            System.out.println("Column " + i + " " + resultSet.getMetaData().getColumnName(i));

        }
    }

    private void writeResultSet(ResultSet resultSet) throws SQLException {
        // ResultSet is initially before the first data set
        System.out.println("\nStädte Abfrage nach Population und ob es eine Hauptstadt ist\n");
        int counter = 0;
        while (resultSet.next()) {
            ++counter;
            // It is possible to get the columns via name
            // also possible to get the columns via the column number
            // which starts at 1
            // e.g. resultSet.getSTring(2);
//            Date date = resultSet.getDate("datum");
//            String comment = resultSet.getString("comments");

            // Schreibt SpaltenNamen automatisch
//            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
//                System.out.println(resultSet.getMetaData().getColumnName(i) + ": " + resultSet.getString(i));
//            }

            System.out.printf("%3d. ", counter);
            System.out.printf("%-40s hat eine Population von ", resultSet.getString(1));
            int population = resultSet.getInt(2);
            if (population < 1000000) {
                System.out.printf("%8d \n", population);
            } else {
                System.out.printf("%7.2fM \n", population / 1000000f);
            }
            if (resultSet.getString(3) != null) {
                System.out.println("     " + resultSet.getString(1) + " ist die Hauptstadt von " + resultSet.getString(3));
            }
        }
        System.out.println("\n     Es wurden mit Ihrer Eingabe  " + counter + " Städte gefunden.");
    }

    // You need to close the resultSet
    private void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }

            if (statement != null) {
                statement.close();
            }

            if (connect != null) {
                connect.close();
            }
        } catch (Exception e) {
        }
    }


}
