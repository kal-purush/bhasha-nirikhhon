import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class BaseDeDatos {
    private static Connection connection;
    private static String url;

    BaseDeDatos() {
        url = "jdbc:mysql://localhost:3337/tricount";
    }

    //Iniciar la conexión con la base de datos
    public static void iniciarSesion() {
        connection = null;
        try {
            connection = DriverManager.getConnection(url, "root", "root");
            System.out.println("Conexion establecida");
        } catch (SQLException e) {
            System.out.println("Ha ocurrido un error al conectarse");
        }
    }

    //Finalizar la conexión con la base de datos
    public static void cerrarSesion() {
        try {
            connection.close();
            System.out.println("Conexion cerrada");
        } catch (SQLException e) {
            System.out.println("Ha ocurrido un error al cerrar la sesión");
        }
    }

    //Insertar valores en la base de datos
    public static void insertarValores(String sentencia) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.executeUpdate(sentencia);
            System.out.println("Datos insertados correctamente");
        } catch (SQLException e) {
            System.out.println("Ha ocurrido un error al introducir los datos");
        }

    }
}