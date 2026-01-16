package SQL;
import java.sql.*;

public class test {
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

            stat.executeUpdate("INSERT INTO turnover (product_name, price, sales)"
                    + " VALUES ('Кроссовки', 450, 36);");
            stat.executeUpdate("INSERT INTO turnover (product_name, price, sales)"
                    + " VALUES ('Колбаса', 150, 123);");
            stat.executeUpdate("INSERT INTO turnover (product_name, price, sales)"
                    + " VALUES ('Хлеб', 21, 367);");
            stat.executeUpdate("INSERT INTO turnover (product_name, price, sales)"
                    + " VALUES ('Сникерс', 16, 91);");
            stat.executeUpdate("INSERT INTO turnover (product_name, price, sales)"
                    + " VALUES ('Сметана', 42, 41);");
            stat.executeUpdate("INSERT INTO turnover (product_name, price, sales)"
                    + " VALUES ('Молоток', 98, 3);");
            stat.executeUpdate("INSERT INTO turnover (product_name, price, sales)"
                    + " VALUES ('Батут', 1800, 1);");
            stat.executeUpdate("INSERT INTO turnover (product_name, price, sales)"
                    + " VALUES ('Сухарики', 29, 112);");
            stat.executeUpdate("INSERT INTO turnover (product_name, price, sales)"
                    + " VALUES ('Тарелка', 53, 9);");
            stat.executeUpdate("INSERT INTO turnover (product_name, price, sales)"
                    + " VALUES ('Пончик', 19, 77);");



            ResultSet rs = stat.executeQuery("SELECT * FROM turnover"
                    + " WHERE sales=(SELECT MAX(sales) FROM turnover)");
            System.out.printf("%s\t  %s\t  %s\t  %s%n", "№", "Товар", "Цена", "Продажи");
            while (rs.next()) {
                System.out.printf("%s\t  %s\t  %s\t  %s%n", rs.getString("product_id"),
                        rs.getString("product_name"), rs.getString("price"),
                        rs.getString("sales"));
            }
            System.out.println("______________________________");
            rs = stat.executeQuery("SELECT SUM(price) as sum FROM turnover");
            rs.next();
            System.out.println("Сумма всех цен - " + rs.getString("sum"));
            System.out.println("______________________________");
            rs = stat.executeQuery("SELECT * FROM turnover ORDER BY product_name");
            while (rs.next()) {
                System.out.printf("%s\t  %s\t  %s\t  %s%n", rs.getString("product_id"),
                        rs.getString("product_name"), rs.getString("price"),
                        rs.getString("sales"));
            }
            System.out.println("______________________________");
            System.out.println("Товарьі продажи которьіх больше 50, и цена больше 10 но меньше 80:");
            rs = stat.executeQuery("SELECT * FROM turnover WHERE sales > 50 AND price BETWEEN 10 AND 80");
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