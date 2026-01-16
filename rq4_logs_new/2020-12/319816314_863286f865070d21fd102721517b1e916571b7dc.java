package com.java.excercises.school;

import java.awt.BorderLayout;
import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.JButton;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import java.awt.Color;
import javax.swing.JLabel;
import javax.swing.ImageIcon;
import javax.swing.SwingConstants;
import javax.swing.JTextField;
import javax.swing.JComboBox;
import javax.swing.JTextArea;
import java.awt.SystemColor;
import java.awt.Font;
import javax.swing.DefaultComboBoxModel;

public class SchoolApp extends JFrame {

	private JPanel contentPane;
	private JTextField txtidStudent;
	private JTextField txtStudentName;
	private JTextField txtPayment;
	private JTextField txtGetStudentId;
	private JComboBox<String>cmbCareer = new JComboBox<String>();
	private JTextArea txtList;
	SchoolManagement manager = new SchoolManagement();

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					SchoolApp frame = new SchoolApp();
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
	public SchoolApp() {
		setResizable(false);
		setTitle("Sistema de Pagos");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 421, 493);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPane.setLayout(new BorderLayout(0, 0));
		setContentPane(contentPane);
		
		JPanel panel = new JPanel();
		panel.setBackground(Color.WHITE);
		contentPane.add(panel, BorderLayout.CENTER);
		
		JButton btnCreatePayment = new JButton("Generar Pago");
		btnCreatePayment.setBounds(188, 195, 114, 23);
		btnCreatePayment.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				Student studentObj = new Student();
				studentObj.setIdStudent(Integer.parseInt(txtidStudent.getText()));
				studentObj.setStudentName(txtStudentName.getText());
				studentObj.setCareer((String)cmbCareer.getSelectedItem());
				studentObj.setPayment(Double.parseDouble(txtPayment.getText()));

				manager.addStudentToList(studentObj);
				
				JOptionPane.showMessageDialog(null, "Pago realizado con éxito");
				
				txtidStudent.setText(null);
				txtStudentName.setText(null);
				cmbCareer.setSelectedItem(0);
				txtPayment.setText(null);
			}
		});
		panel.setLayout(null);
		panel.add(btnCreatePayment);
		
		JLabel lblLogo = new JLabel("");
		lblLogo.setIcon(new ImageIcon(SchoolApp.class.getResource("/com/java/excercises/school/TESCI.jpg")));
		lblLogo.setBounds(267, 45, 123, 100);
		panel.add(lblLogo);
		
		JLabel lblNewLabel_1 = new JLabel("idAlumno:");
		lblNewLabel_1.setBounds(28, 97, 65, 14);
		panel.add(lblNewLabel_1);
		
		JLabel lblNewLabel_1_1 = new JLabel("Nombre:");
		lblNewLabel_1_1.setBounds(28, 131, 65, 14);
		panel.add(lblNewLabel_1_1);
		
		JLabel lblNewLabel_1_2 = new JLabel("Carrera:");
		lblNewLabel_1_2.setBounds(28, 166, 65, 14);
		panel.add(lblNewLabel_1_2);
		
		JLabel lblNewLabel_1_2_1 = new JLabel("Pago: $");
		lblNewLabel_1_2_1.setBounds(28, 199, 46, 14);
		panel.add(lblNewLabel_1_2_1);
		
		txtidStudent = new JTextField();
		txtidStudent.setBounds(92, 94, 59, 20);
		panel.add(txtidStudent);
		txtidStudent.setColumns(10);
		
		txtStudentName = new JTextField();
		txtStudentName.setColumns(10);
		txtStudentName.setBounds(92, 125, 86, 20);
		panel.add(txtStudentName);
		
		txtPayment = new JTextField();
		txtPayment.setColumns(10);
		txtPayment.setBounds(92, 196, 86, 20);
		panel.add(txtPayment);
		
		JComboBox cmdCareer = new JComboBox();
		cmdCareer.setModel(new DefaultComboBoxModel(new String[] {"Selecciona Carrera...", "Ing. en Sistemas Computacionales", "Lic. en Derecho", "Ing. Mecánica", "Lic. en Turismo", "Lic. en Administración"}));
		cmdCareer.setBounds(92, 162, 158, 22);
		panel.add(cmdCareer);
		
		JTextArea txtList = new JTextArea();
		txtList.setBackground(SystemColor.controlHighlight);
		txtList.setBounds(28, 271, 340, 105);
		panel.add(txtList);
		
		JButton btnShowAll = new JButton("Mostrar Todos");
		btnShowAll.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				
				
				txtList.setText(manager.showStudentList().toString());
			}
		});
		btnShowAll.setBounds(145, 386, 114, 23);
		panel.add(btnShowAll);
		
		txtGetStudentId = new JTextField();
		txtGetStudentId.setColumns(10);
		txtGetStudentId.setBounds(164, 240, 86, 20);
		panel.add(txtGetStudentId);
		
		JButton btnSearch = new JButton("Buscar");
		btnSearch.setBounds(260, 237, 79, 23);
		panel.add(btnSearch);
		
		JButton btnClose = new JButton("Salir");
		btnClose.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				System.exit(0);
			}
		});
		btnClose.setBounds(320, 410, 65, 23);
		panel.add(btnClose);
		
		JLabel lblNewLabel_1_3 = new JLabel("Tecnológico de Estudios Superiores de Cuautitlán Izcalli");
		lblNewLabel_1_3.setFont(new Font("Arial Black", Font.BOLD, 12));
		lblNewLabel_1_3.setBounds(10, 0, 371, 34);
		panel.add(lblNewLabel_1_3);
		
		JLabel lblNewLabel_1_3_1 = new JLabel("Control de Pagos");
		lblNewLabel_1_3_1.setFont(new Font("Arial Black", Font.BOLD, 13));
		lblNewLabel_1_3_1.setBounds(126, 28, 123, 34);
		panel.add(lblNewLabel_1_3_1);
		
		JLabel lblNewLabel_1_2_2 = new JLabel("Consulta por idAlumno:");
		lblNewLabel_1_2_2.setBounds(28, 246, 136, 14);
		panel.add(lblNewLabel_1_2_2);
		
	}
}