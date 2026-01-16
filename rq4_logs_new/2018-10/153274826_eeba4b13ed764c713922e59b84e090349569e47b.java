package at.htl.library;

import org.apache.derby.client.am.DateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class LibraryTest {

    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db;create=true";
    static final String USER="app";
    static final String PASSWORD="app";
    private static Connection conn;


    @BeforeClass
    public static void initJdbc() {
        try{
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING,USER,PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("Verbindung zur Datenbank nicht möglich:\n"+e.getMessage()+"\n");
            System.exit(1);
        }

        try{
            Statement stmt = conn.createStatement();

            String sql = "CREATE TABLE BOOK (" +
                    "id int constraint book_pk primary key generated always as identity (start with 1, increment by 1)," +
                    "title varchar(255) not null," +
                    "author varchar(255) not null," +
                    "price double not null)";

            stmt.execute(sql);
            sql = "CREATE TABLE Exemplar (" +
                    "id int constraint exemplar_pk primary key generated always as identity (start with 1, increment by 1)," +
                    "book_id int constraint book_fk references book ," +
                    "condition varchar(255) not null)";
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void dml(){
        int countInserts = 0;
        try{
            Statement stmt = conn.createStatement();
            String sql = "INSERT INTO book (title,author,price) values ('Lord of the Rings','J.R.R. Tolkien',25.00)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO book (title,author,price) values ('The Hobbit','J.R.R. Tolkien',10.00)";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO exemplar (book_id,condition) values (1,'damaged')";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO exemplar (book_id,condition) values (1,'new')";
            countInserts += stmt.executeUpdate(sql);
            sql = "INSERT INTO exemplar (book_id,condition) values (2,'damaged')";
            countInserts += stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat(countInserts,is(5));

        try {
            PreparedStatement pstmt = conn.prepareStatement("Select id,title,author,price from book");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("title"),is("Lord of the Rings"));
            assertThat(rs.getString("author"),is("J.R.R. Tolkien"));
            assertThat(rs.getDouble("price"),is(25.00));
            rs.next();
            assertThat(rs.getString("title"),is("The Hobbit"));
            assertThat(rs.getString("author"),is("J.R.R. Tolkien"));
            assertThat(rs.getDouble("price"),is(10.00));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            PreparedStatement pstmt = conn.prepareStatement("Select id,book_id,condition from exemplar");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getInt("book_id"),is(1));
            assertThat(rs.getString("condition"),is("damaged"));
            rs.next();
            assertThat(rs.getInt("book_id"),is(1));
            assertThat(rs.getString("condition"),is("new"));
            rs.next();
            assertThat(rs.getInt("book_id"),is(2));
            assertThat(rs.getString("condition"),is("damaged"));
            rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void metadata(){
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("Select * from book");
            ResultSetMetaData rsmd = rs.getMetaData();

            assertThat(rsmd.getColumnCount(),is(4));
            assertThat(rsmd.getColumnName(1),is("ID"));
            assertThat(rsmd.getColumnName(2),is("TITLE"));
            assertThat(rsmd.getColumnName(3),is("AUTHOR"));
            assertThat(rsmd.getColumnName(4),is("PRICE"));

            assertThat(rsmd.getColumnTypeName(1),is("INTEGER"));
            assertThat(rsmd.getColumnTypeName(2),is("VARCHAR"));
            assertThat(rsmd.getColumnTypeName(3),is("VARCHAR"));
            assertThat(rsmd.getColumnTypeName(4),is("DOUBLE"));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("Select * from Exemplar");
            ResultSetMetaData rsmd = rs.getMetaData();

            assertThat(rsmd.getColumnCount(),is(3));
            assertThat(rsmd.getColumnName(1),is("ID"));
            assertThat(rsmd.getColumnName(2),is("BOOK_ID"));
            assertThat(rsmd.getColumnName(3),is("CONDITION"));

            assertThat(rsmd.getColumnTypeName(1),is("INTEGER"));
            assertThat(rsmd.getColumnTypeName(2),is("INTEGER"));
            assertThat(rsmd.getColumnTypeName(3),is("VARCHAR"));

            DatabaseMetaData dbmd = conn.getMetaData();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {/
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select c.constraintname, c.constraintid " +
                    "from sys.sysconstraints c, sys.systables t " +
                    "where t.tableid=c.tableid " +
                    "and t.tablename='BOOK'");
            rs.next();
            assertThat(rs.getString(1),is("BOOK_PK"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select c.constraintname, c.constraintid " +
                    "from sys.sysconstraints c, sys.systables t " +
                    "where t.tableid=c.tableid " +
                    "and t.tablename='EXEMPLAR'");
            rs.next();
            assertThat(rs.getString(1),is("EXEMPLAR_PK"));
            rs.next();
            assertThat(rs.getString(1),is("BOOK_FK"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @AfterClass
    public static void teardownJdbc(){
        try{
            conn.createStatement().execute("DROP TABLE Exemplar");
            System.out.println("Tabelle exemplar gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle exemplar konnte nicht gelöscht werden:\n"+e.getMessage()+"\n");
        }
        try{
            conn.createStatement().execute("DROP TABLE book");
            System.out.println("Tabelle book gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle book konnte nicht gelöscht werden:\n"+e.getMessage()+"\n");
        }

        try {
            if (conn !=null || !conn.isClosed()){
                conn.close();
                System.out.println("Goodbye");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}