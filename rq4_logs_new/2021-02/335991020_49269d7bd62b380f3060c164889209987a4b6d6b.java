package GeekBrians.Slava_5655380.Homework.Lesson9;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;

// ЗАДАНИЕ 1. Создать класс кота
public class Cat {
    private final int id;
    private String name;
    private String breed;
    private int age;

    public Cat(String name, String breed, int age) {
        id = -1;
        this.name = name;
        this.breed = breed;
        this.age = age;
    }

    public Cat(int id, String name, String breed, int age) {
        this.id = id;
        this.name = name;
        this.breed = breed;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public String getBreed() {
        return breed;
    }

    public int getAge() {
        return age;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setBreed(String breed) {
        this.breed = breed;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return ((id != -1)?"#" + id + " ":"") + "Кот " + name + " из породы " + breed + " возрастом в " + age + " лет";
    }

    public static Connection getConnectionToCatsDB() throws ClassNotFoundException, SQLException {
        Class.forName("org.sqlite.JDBC");
        File catsDB = new File("src\\main\\java\\GeekBrians\\Slava_5655380\\Homework\\Lesson9\\Cats.db");
        if(catsDB.exists())
            catsDB.delete();
        Connection connection = DriverManager.getConnection("jdbc:sqlite:src\\main\\java\\GeekBrians\\Slava_5655380\\Homework\\Lesson9\\Cats.db");
        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE if not exists Cats(CatID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, Name TEXT NOT NULL, Breed TEXT NOT NULL, Age INTEGER NOT NULL);");
        return connection;
    }

    public static void printCatsDB(Connection connection) throws SQLException {
        // ЗАДАНИЕ 3. Написать метод извлечения котов
        Cat[] catsFormDB = Cat.getFromCatsDB(connection);
        for (Cat cat : catsFormDB)
            System.out.println(cat);
    }

    // ЗАДАНИЕ 3. Написать метод извлечения котов
    public static Cat[] getFromCatsDB(Connection connection) throws SQLException {
        return getFromCatsDB(connection, "");
    }


    public static Cat[] getFromCatsDB(Connection connection, String name) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM Cats" + ((name.length() == 0) ? ";" : "WHERE Name = '" + name + "';"));
        ArrayList<Cat> catList = new ArrayList<Cat>();
        while (resultSet.next())
            catList.add(
                    new Cat(
                            resultSet.getInt("CatID"),
                            resultSet.getString("Name"),
                            resultSet.getString("Breed"),
                            resultSet.getInt("Age")
                    )
            );
        // return (Cat[]) (catList.toArray());
        Cat[] cats = new Cat[catList.size()];
        for (int i = 0; i < catList.size(); i++)
            cats[i] = catList.get(i);
        return cats;
    }

    // ЗАДАНИЕ 4. Метод добавления котов
    public static void addToCatsDB(Connection connection, Cat cat) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("insert into Cats(Name, Breed, Age) values (?, ?, ?)");
        preparedStatement.setString(1, cat.name);
        preparedStatement.setString(2, cat.breed);
        preparedStatement.setInt(3, cat.age);
        preparedStatement.execute();
    }

    public static void addToCatsDB(Connection connection, Cat[] cats) throws SQLException {
        connection.setAutoCommit(false);
        for (Cat cat : cats)
            addToCatsDB(connection, cat);
        connection.commit();
        connection.setAutoCommit(true);
    }

    // ЗАДАНИЕ 5. Метод удаления котов
    public static void rmFromCatsDB(Connection connection, int id) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("DELETE FROM Cats WHERE CatID = " + id);
    }

    // ЗАДАНИЕ 6. Метод изменения котов
    public static void updAgeInCatDB(Connection connection, int id, int age) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("UPDATE Cats SET Age = " + age + " WHERE CatID = " + id + ";");
    }
}