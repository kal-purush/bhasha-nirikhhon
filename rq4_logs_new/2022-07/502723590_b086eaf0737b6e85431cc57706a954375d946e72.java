package SQL;

import java.sql.*;

public class SQL_task_2 {
    public static void main(String[] args) {
        try {
            String url = "jdbc:mysql://localhost/ms_db?serverTimezone=Europe/Kiev&useSSL=false&useUnicode=true&characterEncoding=utf-8";
            String username = "root";
            String password = "root";
            Connection conn = DriverManager.getConnection(url, username, password);
            Statement stat = conn.createStatement();
            stat.executeUpdate("DROP TABLE IF EXISTS turnover;");
            stat.executeUpdate("CREATE TABLE turnover "
                    + "(product_id INT NOT NULL AUTO_INCREMENT, product_name VARCHAR(255), price INT, "
                    + "sales INT, PRIMARY KEY (product_id));");
            PreparedStatement prep = conn.prepareStatement("INSERT INTO turnover (product_name, price, sales) VALUES (?, ?, ?);");
            prep.setString(1, "Кроссовки");
            prep.setInt(2, 450);
            prep.setInt(3, 36);
            prep.addBatch();
            prep.setString(1, "Колбаса");
            prep.setInt(2, 150);
            prep.setInt(3, 123);
            prep.addBatch();
            prep.setString(1, "Хлеб");
            prep.setInt(2, 21);
            prep.setInt(3, 367);
            prep.addBatch();
            prep.setString(1, "Сникерс");
            prep.setInt(2, 16);
            prep.setInt(3, 91);
            prep.addBatch();
            prep.setString(1, "Сметана");
            prep.setInt(2, 42);
            prep.setInt(3, 41);
            prep.addBatch();
            prep.setString(1, "Молоток");
            prep.setInt(2, 98);
            prep.setInt(3, 3);
            prep.addBatch();
            prep.setString(1, "Батут");
            prep.setInt(2, 1800);
            prep.setInt(3, 1);
            prep.addBatch();
            prep.setString(1, "Сухарики");
            prep.setInt(2, 29);
            prep.setInt(3, 112);
            prep.addBatch();
            prep.setString(1, "Тарелка");
            prep.setInt(2, 53);
            prep.setInt(3, 9);
            prep.addBatch();
            prep.setString(1, "Пончик");
            prep.setInt(2, 19);
            prep.setInt(3, 77);
            prep.executeBatch();

            prep = conn.prepareStatement("SELECT * FROM turnover"
                    + " WHERE sales=(SELECT MAX(sales) FROM turnover)");
            ResultSet rs = prep.executeQuery();
            System.out.printf("%s\t  %s\t  %s\t  %s%n", "№", "Товар", "Цена", "Продажи");
            while (rs.next()) {
                System.out.printf("%s\t  %s\t  %s\t  %s%n", rs.getString("product_id"),
                        rs.getString("product_name"), rs.getString("price"),
                        rs.getString("sales"));
            }
            System.out.println("______________________________");
            prep = conn.prepareStatement("SELECT SUM(price) as sum FROM turnover");
            rs = prep.executeQuery();
            rs.next();
            System.out.println("Сумма всех цен - " + rs.getString("sum"));
            System.out.println("______________________________");
            prep = conn.prepareStatement("SELECT * FROM turnover ORDER BY product_name");
            rs = prep.executeQuery();
            while (rs.next()) {
                System.out.printf("%s\t  %s\t  %s\t  %s%n", rs.getString("product_id"),
                        rs.getString("product_name"), rs.getString("price"),
                        rs.getString("sales"));
            }
            System.out.println("______________________________");
            System.out.println("Товарьі продажи которьіх больше 50, и цена больше 10 но меньше 80:");
            prep = conn.prepareStatement("SELECT * FROM turnover WHERE sales > 50 AND price BETWEEN 10 AND 80");
            rs = prep.executeQuery();
            while (rs.next()) {
                System.out.printf("%s\t  %s\t  %s\t  %s%n", rs.getString("product_id"),
                        rs.getString("product_name"), rs.getString("price"),
                        rs.getString("sales"));
            }
            rs.close();
            conn.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}