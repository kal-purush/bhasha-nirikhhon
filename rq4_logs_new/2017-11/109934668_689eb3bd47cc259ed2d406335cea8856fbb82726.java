package base;

import clase.Asesor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author Parker
 */
public class AsesorDBHelper {
    /**
    * Se utuliza para hacer conexión con la base de datos.
    */
    static DBConexion conexion = null;

    /**
    * Constructos de AsesorDBHelper.
    */
    public AsesorDBHelper() {
        conexion = new DBConexion();
    }

    /**
    * Registra un asesor en la base de datos.
    *
    * @param    asesor  objeto Asesor el cual se guardara en la base de datos.
    * @return           un boolean, true si se guardo exitosamente,
    *                   false si ocurrio un error.
    */
    public final boolean registrar(final Asesor asesor) {
        boolean transaccion = true;

        try {
            Statement sentencia = conexion.getStatement();

            String query = "INSERT INTO ASESOR VALUES "
                        + "((SELECT COUNT(*) FROM ASESOR) + 1, '"
                        + asesor.getNombre() + "', '"
                        + asesor.getApellidoPaterno() + "', '"
                        + asesor.getApellidoMaterno() + "', '"
                        + asesor.getContrasena() + "', '"
                        + asesor.getCubiculo() + "', '"
                        + asesor.getEmail() + "', "
                        + "TO_DATE('0001-01-01 " + asesor.getHrInicioAsesoria()
                        + "','YYYY-MM-DD HH24:MI:SS'), "
                        + "TO_DATE('0001-01-01 " + asesor.getHrFinAsesoria()
                        + "','YYYY-MM-DD HH24:MI:SS'))";

            sentencia.execute(query);
            confirmar();

        } catch (SQLException ex) {
            System.out.println("BaseDatos.AsesorDBHelper.registrar() " + ex);
            transaccion = false;
        }

        return transaccion;
    }

    /**
    * Obtiene el id de un asesor por su contraseña.
    *
    * @param    asesor  objeto Asesor el cual se obtendra su id que se encuentra
    *                   en la base de datos.
    * @return           un int, si es 0 ocurrio un error o no se encontro el
    *                   asesor, de lo contrario regresa el id del asesor.
    */
    public final int getIdAsesor(final Asesor asesor) {
        int id = 0;

        try {
            Statement sentencia = conexion.getStatement();

            String query = "SELECT IDASESOR FROM ASESOR "
                        + "WHERE CONTRASENA = '" + asesor.getContrasena() + "'";
            ResultSet resultado = sentencia.executeQuery(query);

            while (resultado.next()) {
                id = resultado.getInt("IDASESOR");
            }
        } catch (SQLException ex) {
            System.out.println("BaseDatos.AsesorDBHelper.getIdAsesor() " + ex);
        }

        return id;
    }

    /**
    * Se Obtiene un asesor por su contraseña.
    *
    * @param    contrasena  String con la cual se realizara la busqueda en la
    *                       base de datos.
    * @return               un Asesor, si el objeto es null no se encontro.
    */
    public final Asesor getAsesor(final String contrasena) {
        Asesor asesor = null;

        try {
            Statement sentencia = conexion.getStatement();

            String query = "SELECT * FROM ASESOR "
                        + "WHERE CONTRASENA = '" + contrasena + "'";
            ResultSet resultado = sentencia.executeQuery(query);

            while (resultado.next()) {
                int idAsesor = resultado.getInt("IDASESOR");
                String nombre = resultado.getString("NOMBRE");
                String apPaterno = resultado.getString("APELLIDOPATERNO");
                String apMaterno = resultado.getString("APELLIDOMATERNO");
                String cubiculo = resultado.getString("CUBICULO");
                String email = resultado.getString("EMAIL");

                asesor = new Asesor(nombre, apPaterno, apMaterno, contrasena);
                asesor.setId(idAsesor);
                asesor.setCubiculo(cubiculo);
                asesor.setEmail(email);
            }
        } catch (SQLException ex) {
            System.out.println("BaseDatos.AsesorDBHelper.getAsesor() " + ex);
        }

        return asesor;
    }

    /**
    * Se Obtiene un asesor por su id.
    *
    * @param    id  int con el cual se realizara la busqueda en la
    *               base de datos.
    * @return       un Asesor, si el objeto es null no se encontro.
    */
    public final Asesor getAsesor(final int id) {
        Asesor asesor = null;

        try {
            Statement sentencia = conexion.getStatement();

            String query = "SELECT * FROM ASESOR WHERE IDASESOR = " + id;
            ResultSet resultado = sentencia.executeQuery(query);

            while (resultado.next()) {
                String nombre = resultado.getString("NOMBRE");
                String apPaterno = resultado.getString("APELLIDOPATERNO");
                String apMaterno = resultado.getString("APELLIDOMATERNO");
                String cubiculo = resultado.getString("CUBICULO");
                String email = resultado.getString("EMAIL");

                asesor = new Asesor(nombre, apPaterno, apMaterno, "");
                asesor.setId(id);
                asesor.setCubiculo(cubiculo);
                asesor.setEmail(email);
            }
        } catch (SQLException ex) {
            System.out.println("BaseDatos.AsesorDBHelper.getAsesor() " + ex);
        }

        return asesor;
    }

    /**
    * Realiza un commit en la base de datos.
    *
    * @return       un boolean, true si se realizao un commit exitosamente,
    *               false si ocurrio un error.
    */
    public final boolean confirmar() {
        boolean confirmacion = true;
        try {
            Statement sentencia = conexion.getStatement();

            String query = "COMMIT";
            sentencia.execute(query);
        } catch (SQLException ex) {
            System.out.println("BaseDatos.AsesorDBHelper.confirmar() " + ex);
            confirmacion = false;
        }
        return confirmacion;
    }
}