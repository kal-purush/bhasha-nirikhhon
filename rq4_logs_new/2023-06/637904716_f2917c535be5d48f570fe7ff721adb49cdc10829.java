package vista;

import javax.swing.JInternalFrame;
import javax.swing.JTextField;
import javax.swing.JPanel;
import javax.swing.JLabel;
import javax.swing.JButton;
import java.awt.GridLayout;
import java.awt.BorderLayout;

public class VentanaCliente extends JInternalFrame {

    private JTextField txfId = new JTextField(10);
    private JTextField txfNombre = new JTextField(10);
    private JTextField txfPrecio = new JTextField(10);
    private JTextField txfCategoria = new JTextField(10);
    private JTextField txfExistencia = new JTextField(10);

    public VentanaCliente() {

        super("Registro de producto", false, true, false, true);

        add(crearPanelPrincipal());

        setSize(300, 150);

    }

    private JPanel crearPanelEdicion() {

        JPanel pnlEdicion = new JPanel();

        JLabel lblId = new JLabel("ID del producto: ");

        JLabel lblNombre = new JLabel("Nombre: ");

        JLabel lblPrecio = new JLabel("Precio: ");

        JLabel lblCategoria = new JLabel("Categoria: ");

        JLabel lblExistencia = new JLabel("Existencias: ");

        pnlEdicion.setLayout(new GridLayout(5, 5));

        pnlEdicion.add(lblId);

        pnlEdicion.add(txfId);

        pnlEdicion.add(lblNombre);

        pnlEdicion.add(txfNombre);

        pnlEdicion.add(lblPrecio);

        pnlEdicion.add(txfPrecio);

        pnlEdicion.add(lblCategoria);

        pnlEdicion.add(txfCategoria);

        pnlEdicion.add(lblExistencia);

        pnlEdicion.add(txfExistencia);

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