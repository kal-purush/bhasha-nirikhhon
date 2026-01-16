package lesson_2;


import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.*;
import java.util.Scanner;

// Доп ДЗ
// SELECT name,max(timestamp) FROM dop_dz GROUP BY name;
// где dop_dz - имя таблицы, name - поле типа String, timestamp - поле типа DATE (даты в исходных данных необходимо привести к формату типа DATE)

public class lesson2 {

    private static Connection connection;
    private static Statement stmt;
    private static PreparedStatement pstmt;

    public static void main(String[] args)  {

        try {
            connect();

            stmt.executeUpdate("DROP TABLE IF EXISTS prodtable");
            stmt.executeUpdate("CREATE TABLE prodtable (\n" +
                    "    id        INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
                    "    prodid      INTEGER,\n" +
                    "    title      STRING,\n" +
                    "    cost INTEGER UNSIGNED\n" +
                    ");");

            connection.setAutoCommit(false);

            pstmt = connection.prepareStatement("INSERT INTO prodtable (prodid, title, cost) VALUES (?,?,?)");

            for (int i = 1; i <= 10000; i++) {

                pstmt.setString(1, "id_товара " +i);
                pstmt.setString(2, "товар" + i);
                pstmt.setInt(3, i*10);
                pstmt.addBatch();

            }

            pstmt.executeBatch();

            connection.setAutoCommit(true);
            String command = "";
            Scanner input = new Scanner(System.in);

            while (!(command.equals("quit"))) {

                System.out.print("Введите команду (quit для выхода): ");
                command = input.nextLine();

                if (command.startsWith("цена")) {
                    String[] words = command.split(" ");
                    search(words[1]);
                } else if (command.startsWith("сменитьцену")) {
                    String[] words = command.split(" ");
                    change(words[1], Integer.parseInt(words[2]));
                } else if (command.startsWith("товарыпоцене")) {
                    String[] words = command.split(" ");
                    costDelta(Integer.parseInt(words[1]), Integer.parseInt(words[2]));
                } else if (command.equals("quit")) System.out.println("Выход из программы");
                else System.out.println("команда не найдена");
            }

        } catch (ClassNotFoundException e) {

            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                disconnect();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


    }

    public static void search (String prodid) throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM prodtable WHERE title =\'" + prodid + "\'");

        if (rs.isFirst()) System.out.println("Такого товара нет");
        else {
            while (rs.next()) System.out.println(rs.getString(2) + " " + rs.getString(3) + " " + rs.getInt(4));
        }
    }

    public static void change (String prodid, int cost) throws SQLException {
        stmt.executeUpdate("UPDATE prodtable SET cost = \'"+ cost + "\' WHERE title=\'" + prodid +"\'");
    }

    public static void costDelta (int cost1, int cost2) throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM prodtable WHERE cost BETWEEN \'" + cost1 + "\' AND \'" + cost2 + "\'");

        if (rs.isFirst()) System.out.println("Такого товара нет");
        else {
            while (rs.next()) System.out.println(rs.getString(2) + " " + rs.getString(3) + " " + rs.getInt(4));
        }
    }



    public static void connect() throws ClassNotFoundException, SQLException {
        Class.forName("org.sqlite.JDBC");
        connection = DriverManager.getConnection("jdbc:sqlite:D:\\Dropbox\\GB\\java3.db");
        stmt = connection.createStatement();

    }

    public static void disconnect() throws SQLException {
        connection.close();
    }
}

