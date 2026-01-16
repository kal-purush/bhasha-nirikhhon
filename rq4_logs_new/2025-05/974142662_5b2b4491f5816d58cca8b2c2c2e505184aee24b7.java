package Pantallas;

import java.awt.EventQueue;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.JTextArea;
import javax.swing.JButton;
import java.awt.Font;
import java.awt.SystemColor;
import javax.swing.JTextField;
import javax.swing.SwingConstants;

public class Cobro extends JFrame {

	private static final long serialVersionUID = 1L;
	private JPanel contentPane;
	private JTextField texto;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					Cobro frame = new Cobro();
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
	public Cobro() {
		setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		setBounds(100, 100, 900, 700);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));

		setContentPane(contentPane);
		contentPane.setLayout(null);
		
		
		
		
		JButton Pagar = new JButton("Pagar");
		Pagar.setFont(new Font("Tahoma", Font.BOLD, 25));
		Pagar.setBackground(SystemColor.info);
		Pagar.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				try {
					x.conectar();
					String sentencia= "DELETE FROM seleccion_producto WHERE Identificador= '"+Restaurante.mesa+"'";
					x.ejecutarInsertDeleteUpdate(sentencia);
					dispose();				
					} 
				catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
			}
		});
		Pagar.setBounds(343, 476, 197, 55);
		contentPane.add(Pagar);
		
		texto = new JTextField();
		texto.setHorizontalAlignment(SwingConstants.CENTER);
		texto.setFont(new Font("Calibri", Font.BOLD, 77));
		texto.setBackground(SystemColor.info);
		texto.setBounds(314, 214, 254, 101);
		contentPane.add(texto);
		texto.setColumns(10);
		texto.setText(Integer.toString(Restaurante.total)+"€");
		
		
		
		JLabel lblNewLabel = new JLabel("");
		lblNewLabel.setIcon(new ImageIcon(MesaResumen.class.getResource("/Imagenes/fondopestana.jpg")));
		lblNewLabel.setBounds(0, 0, 884, 661);
		contentPane.add(lblNewLabel);
		
	}
}