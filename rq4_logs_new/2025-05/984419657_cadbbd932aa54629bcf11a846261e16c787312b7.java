package clasesProyecto;

import java.awt.EventQueue;
import java.awt.Image;
import java.sql.SQLException;

import javax.swing.*;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.Font;

public class pagina03Borrar_Datos extends JFrame {

	private JPanel contentPane;
	private JTextField textField_TemporadaAConsultar;
	private ConexionMySQL conexion;

	public pagina03Borrar_Datos(ConexionMySQL conexion) {
		this.conexion = conexion;

		setTitle("LaLiga Fantasy Simulator");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 450, 300);
		setLocationRelativeTo(null);
		contentPane = new JPanel();
		contentPane.setLayout(null);
		setContentPane(contentPane);

		JLabel lblFoto = new JLabel();
		lblFoto.setBounds(33, 0, 363, 113);
		ImageIcon icono = new ImageIcon(getClass().getResource("/images/RGB_Logotipo_LALIGA_color_positivevg.png"));
		Image imagen = icono.getImage().getScaledInstance(lblFoto.getWidth(), lblFoto.getHeight(), Image.SCALE_SMOOTH);
		lblFoto.setIcon(new ImageIcon(imagen));
		contentPane.add(lblFoto);

		JButton btnBorrarTodas = new JButton("BORRAR TEMPORADA GENERADA");
		btnBorrarTodas.setBounds(101, 202, 212, 23);
		contentPane.add(btnBorrarTodas);

		JButton btnVolver = new JButton("Volver");
		btnVolver.setBounds(10, 227, 68, 23);
		contentPane.add(btnVolver);

		// --- Acción del botón BORRAR TODAS ---
		btnBorrarTodas.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				try {
					if (!conexion.estaConectado()) {
						conexion.conectar();
					}

					int confirmacion = JOptionPane.showConfirmDialog(null, "¿Seguro que quieres eliminar todos los partidos?", "Confirmar", JOptionPane.YES_NO_OPTION);
					if (confirmacion == JOptionPane.YES_OPTION) {
						int filas = conexion.ejecutarInsertDeleteUpdate("DELETE FROM partidos");
						conexion.ejecutarInsertDeleteUpdate("ALTER TABLE partidos AUTO_INCREMENT = 1");

						if (filas > 0) {
							JOptionPane.showMessageDialog(null, "Todos los partidos eliminados correctamente.");
						} else {
							JOptionPane.showMessageDialog(null, "No había partidos para eliminar.");
						}
					}

				} catch (SQLException ex) {
					ex.printStackTrace();
					JOptionPane.showMessageDialog(null, "Error de base de datos: " + ex.getMessage());
				}
			}
		});

		// --- Acción del botón VOLVER ---
		btnVolver.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				pagina01Principal principal = new pagina01Principal(conexion);
				principal.setVisible(true);
				dispose();
			}
		});
	}

	// --- Metodo MAIN para pruebas individuales ---
	public static void main(String[] args) {
		EventQueue.invokeLater(() -> {
			try {
				ConexionMySQL conexion = new ConexionMySQL("root", "1234", "laliga");
				conexion.conectar();
				new pagina03Borrar_Datos(conexion).setVisible(true);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}
}