package daos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import modelos.BalanceGeneral;
import java.sql.SQLException;


/**
 *
 * @author Luis
 */
public class DaoBalanzaGeneral {

    Conexion conexion;
    private ArrayList<BalanceGeneral> listaBalanceGeneral;
    private ResultSet rs = null;
    private PreparedStatement ps;
    private Connection accesoDB;

    private static final String CONSULTAR_BALANZA = "SELECT DISTINCT catalogo.codigo,catalogo.nombrecuenta,(SELECT CAST(sum(librodiario.monto) AS NUMERIC) as total_x_cuenta FROM librodiario\n"
            + "WHERE librodiario.codigo=catalogo.codigo AND catalogo.cuentatipo = 1) \n"
            + "FROM catalogo,librodiario\n"
            + "WHERE catalogo.codigo=librodiario.codigo\n"
            + "ORDER BY catalogo.codigo";

    public ArrayList<BalanceGeneral> CargarBalanceGeneral() {

        this.listaBalanceGeneral = new ArrayList();

        try {
            this.accesoDB = this.conexion.getConexion();
            this.ps = this.accesoDB.prepareStatement(CONSULTAR_BALANZA);
            this.rs = ps.executeQuery();

            BalanceGeneral obj = null;
            while (this.rs.next()) {
                obj = new BalanceGeneral();
                obj.setCodigo(rs.getString("codigo"));
                obj.setCuenta(rs.getString("nombrecuenta"));
                obj.setMonto(Integer.toString(rs.getInt("total_x_cuenta")));
                this.listaBalanceGeneral.add(obj);
            }
            this.conexion.cerrarConexiones();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return this.listaBalanceGeneral;
    }
}