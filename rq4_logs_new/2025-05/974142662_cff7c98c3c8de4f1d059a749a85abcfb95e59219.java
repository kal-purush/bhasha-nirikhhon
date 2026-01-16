package Pantallas;

import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import ConexionBasedeDatos.ConexionMySQL;

import javax.swing.JPasswordField;
import javax.swing.JButton;
import java.awt.Font;
import java.awt.Color;
import javax.swing.UIManager;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JTextField;
import java.awt.Button;
import javax.swing.SwingConstants;
import javax.swing.ImageIcon;
import java.awt.Panel;
import java.awt.Canvas;
import java.awt.event.ActionListener;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.awt.event.ActionEvent;

public class InicioSesion extends JFrame {

	private static final long serialVersionUID = 1L;
	private JPanel contentPane;
	private JPasswordField passwordField;
	private JTextField text_usuario;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					InicioSesion frame = new InicioSesion();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the frame.
	 */
	ConexionBasedeDatos.ConexionMySQL x = new ConexionBasedeDatos.ConexionMySQL("root", "", "Registro");
	public InicioSesion() {
		setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		setBounds(100, 100, 900, 700);
		contentPane = new JPanel();
		contentPane.setBackground(new Color(51, 255, 204));
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));

		setContentPane(contentPane);
		contentPane.setLayout(null);
		
///////////////////////////////////////////////////////////////////////////////////////////BOTONES Y FIELDS/////////////////////////////////////////////////////////////////////////////////////////////////////

		
		Button btn_Acceder = new Button("Acceder");
		btn_Acceder.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				try {
					x.conectar();
					char[] password = passwordField.getPassword();
			        String passwordString = new String(password);
					String z = "SELECT * FROM users";
					ResultSet comparar=x.ejecutarSelect(z);
					boolean encontrado=false;
					while(comparar.next()) 
					{
						String usuario= comparar.getString("Usuario");
						String contraseña= comparar.getString("Contraseña");
						if(usuario.equals(text_usuario.getText())&&contraseña.equals(passwordString))
						{
							encontrado=true;
							break;
						}
					}
					if(encontrado==true) {
						Primera n= new Primera();
						n.setVisible(true);
						}
					else {
							JOptionPane.showMessageDialog(null, "Usuario o contraseña incorrecta.\n"
									+ "Pruebe de nuevo.");
						}
					}
					
					
				 catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
		});
		btn_Acceder.setBounds(415, 495, 88, 29);
		contentPane.add(btn_Acceder);
		
		JButton btn_CrearCuenta = new JButton("Crear Cuenta");
		btn_CrearCuenta.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				Registro r = new Registro();
				r.setVisible(true);
			}
		});
		btn_CrearCuenta.setBackground(new Color(184, 134, 11));
		btn_CrearCuenta.setFont(new Font("Calibri", Font.BOLD, 14));
		btn_CrearCuenta.setBounds(737, 42, 122, 39);
		contentPane.add(btn_CrearCuenta);
		
		text_usuario = new JTextField();
		text_usuario.setBounds(376, 313, 173, 20);
		contentPane.add(text_usuario);
		text_usuario.setColumns(10);
		
		passwordField = new JPasswordField();
		passwordField.setToolTipText("");
		passwordField.setBounds(376, 419, 173, 20);
		contentPane.add(passwordField);
		
		
///////////////////////////////////////////////////////////////////////////////////////////TEXTOS E IMAGENES/////////////////////////////////////////////////////////////////////////////////////////////////////
		JLabel lbl_InicioSesion = new JLabel("Iniciar Sesión");
		lbl_InicioSesion.setBackground(new Color(255, 255, 255));
		lbl_InicioSesion.setHorizontalAlignment(SwingConstants.CENTER);
		lbl_InicioSesion.setFont(new Font("Calibri", Font.BOLD, 35));
		lbl_InicioSesion.setBounds(315, 183, 289, 60);
		contentPane.add(lbl_InicioSesion);
		
		JLabel lbl_Contraseña = new JLabel("Contraseña");
		lbl_Contraseña.setFont(new Font("Calibri", Font.BOLD, 18));
		lbl_Contraseña.setBounds(415, 379, 94, 29);
		contentPane.add(lbl_Contraseña);
		
		JLabel lbl_Usuario = new JLabel("Usuario");
		lbl_Usuario.setFont(new Font("Calibri", Font.BOLD, 18));
		lbl_Usuario.setBounds(431, 273, 104, 29);
		contentPane.add(lbl_Usuario);
		
		JLabel logo = new JLabel("");
		logo.setBackground(Color.BLACK);
		logo.setIcon(new ImageIcon(InicioSesion.class.getResource("/Imagenes/logo.png")));
		logo.setBounds(30, 26, 136, 96);
		contentPane.add(logo);
		
		JLabel lbl_Fondo = new JLabel("New label");
		lbl_Fondo.setIcon(new ImageIcon(InicioSesion.class.getResource("/Imagenes/inicio 2.0 (2).png")));
		lbl_Fondo.setBounds(315, 153, 303, 425);
		contentPane.add(lbl_Fondo);
		
		JLabel lbl_FondoTotal = new JLabel("New label");
		lbl_FondoTotal.setIcon(new ImageIcon(InicioSesion.class.getResource("/Imagenes/fondopestana.jpg")));
		lbl_FondoTotal.setBounds(0, 0, 884, 661);
		contentPane.add(lbl_FondoTotal);
	}
}