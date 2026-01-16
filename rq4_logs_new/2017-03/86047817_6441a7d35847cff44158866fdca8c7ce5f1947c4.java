package vista;

import java.awt.BorderLayout;
import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import controlador.ClienteControlador;
import modelo.Cliente;

import javax.swing.JTextField;
import javax.swing.JLabel;
import javax.swing.JComboBox;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

public class BorradoCliente extends JDialog {

	private final JPanel contentPanel = new JPanel();
	private ClienteControlador clienteControlador;
	private JTextField textoId;
	private JTextField textoNombre;
	private JTextField textoDireccion;
	private JTextField textoCodPostal;
	private JTextField textoTelefono;

	
	/**
	 * Launch the application.
	 */
	
	/**
	 * Create the dialog.
	 */
	public BorradoCliente(GestorCliente padre,boolean modal) {
		super(padre,modal);
		setBounds(100, 100, 450, 300);
		getContentPane().setLayout(new BorderLayout());
		contentPanel.setBorder(new EmptyBorder(5, 5, 5, 5));
		getContentPane().add(contentPanel, BorderLayout.CENTER);
		contentPanel.setLayout(null);
		
		textoId = new JTextField();
		textoId.setBounds(150, 46, 86, 20);
		contentPanel.add(textoId);
		textoId.setColumns(10);
		
		textoNombre = new JTextField();
		textoNombre.setBounds(150, 76, 86, 20);
		contentPanel.add(textoNombre);
		textoNombre.setColumns(10);
		
		textoDireccion = new JTextField();
		textoDireccion.setBounds(150, 107, 86, 20);
		contentPanel.add(textoDireccion);
		textoDireccion.setColumns(10);
		
		textoCodPostal = new JTextField();
		textoCodPostal.setBounds(150, 138, 86, 20);
		contentPanel.add(textoCodPostal);
		textoCodPostal.setColumns(10);
		
		textoTelefono = new JTextField();
		textoTelefono.setBounds(150, 169, 86, 20);
		contentPanel.add(textoTelefono);
		textoTelefono.setColumns(10);
		
		JLabel lblId = new JLabel("Id");
		lblId.setBounds(63, 49, 46, 14);
		contentPanel.add(lblId);
		
		JLabel lblNombre = new JLabel("Nombre");
		lblNombre.setBounds(63, 79, 46, 14);
		contentPanel.add(lblNombre);
		
		JLabel lblDireccion = new JLabel("Direccion");
		lblDireccion.setBounds(63, 110, 46, 14);
		contentPanel.add(lblDireccion);
		
		JLabel lblCodPostal = new JLabel("Cod Postal");
		lblCodPostal.setBounds(63, 141, 46, 14);
		contentPanel.add(lblCodPostal);
		
		JLabel lblTelefono = new JLabel("Telefono");
		lblTelefono.setBounds(63, 172, 46, 14);
		contentPanel.add(lblTelefono);
		
		JComboBox listaClientes = new JComboBox();
		listaClientes.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if(listaClientes.getSelectedIndex() == -1){
					limpiarFormulario();
				}else{
					String datosCliente = (String) listaClientes.getSelectedItem();
					if(datosCliente != null){
						String[] partes = datosCliente.split(":");
						String idCliente = partes[0];
						clienteControlador.rellenarBorradoCliente(idCliente);
				}
				}
			}
		});
		listaClientes.setBounds(276, 11, 116, 20);
		contentPanel.add(listaClientes);
		{
			JPanel buttonPane = new JPanel();
			buttonPane.setLayout(new FlowLayout(FlowLayout.RIGHT));
			getContentPane().add(buttonPane, BorderLayout.SOUTH);
			{
				JButton btnBorrar = new JButton("Borrar");
				btnBorrar.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent arg0) {
						
					}
				});
				btnBorrar.setActionCommand("OK");
				buttonPane.add(btnBorrar);
				getRootPane().setDefaultButton(btnBorrar);
			}
			{
				JButton cancelButton = new JButton("Cancel");
				cancelButton.setActionCommand("Cancel");
				buttonPane.add(cancelButton);
			}
		}
	}
	
	protected void limpiarFormulario() {
		// TODO Auto-generated method stub
		
	}

	public ClienteControlador getClienteControlador() {
		return clienteControlador;
	}

	public void setClienteControlador(ClienteControlador clienteControlador) {
		this.clienteControlador = clienteControlador;
	}
	public void rellenarDatosBorradoCliente(Cliente cliente) {
		// TODO Auto-generated method stub
		textoId.setText(cliente.getId());
		textoNombre.setText(cliente.getNombre());
		textoDireccion.setText(cliente.getDireccion());
		textoCodPostal.setText(cliente.getCodPostal());
		textoTelefono.setText(cliente.getTelefono());
	}
}