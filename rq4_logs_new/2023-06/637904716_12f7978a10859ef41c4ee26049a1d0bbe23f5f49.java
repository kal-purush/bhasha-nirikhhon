package vista;

import javax.swing.JInternalFrame;
import javax.swing.JTextField;
import javax.swing.JPanel;
import javax.swing.JLabel;
import javax.swing.JButton;
import java.awt.GridLayout;
import java.awt.BorderLayout;

public class VentanaPedido extends JInternalFrame {

    private JTextField txfMonto = new JTextField(10);
    private JTextField txfCliente = new JTextField(10);
    private JTextField txfFechaPedido = new JTextField(10);
    private JTextField txfProducto = new JTextField(10);
	private JTextField txfCantidad = new JTextField(10);
   
    public VentanaPedido() {

        super("Registro del Pedido", false, true, false, true);

        add(crearPanelPrincipal());

        setSize(300, 150);

    }

    private JPanel crearPanelEdicion() {

        JPanel pnlEdicion = new JPanel();

        JLabel lblMonto = new JLabel("Monto: ");

        JLabel lblCliente = new JLabel("Cliente: ");

        JLabel lblFechaPedido = new JLabel("Fecha del pedido: ");

        JLabel lblProducto = new JLabel("Producto: ");
		
		JLabel lblCantidad = new JLabel("Cantidad: ");

        pnlEdicion.setLayout(new GridLayout(5, 5));

        pnlEdicion.add(lblMonto);

        pnlEdicion.add(txfMonto);

        pnlEdicion.add(lblCliente);

        pnlEdicion.add(txfCliente);

        pnlEdicion.add(lblFechaPedido);

        pnlEdicion.add(txfFechaPedido);

        pnlEdicion.add(lblProducto);

        pnlEdicion.add(txfProducto);
		
		pnlEdicion.add(lblCantidad);

        pnlEdicion.add(txfCantidad);

        return pnlEdicion;

    }

    public JPanel crearPanelPrincipal() {

        JPanel pnlPrincipal = new JPanel();

        pnlPrincipal.setLayout(new BorderLayout());

        pnlPrincipal.add(crearPanelEdicion(), BorderLayout.CENTER);

        pnlPrincipal.add(crearPanelControl(), BorderLayout.SOUTH);

        return pnlPrincipal;

    }

    public JPanel crearPanelControl() {

        JPanel pnlControl = new JPanel();

        JButton btnAgregar = new JButton("Agregar");

        pnlControl.add(btnAgregar);

        return pnlControl;

    }

}