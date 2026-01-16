package petra.ATTSWproject.view.swing;

import java.awt.Color;

import javax.swing.JFrame;

import petra.ATTSWproject.controller.StudyRoomController;
import petra.ATTSWproject.model.User;
import petra.ATTSWproject.view.StudyRoomView;
import java.awt.GridBagLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;

import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.util.List;
import java.awt.Font;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.border.EmptyBorder;

import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JList;
import javax.swing.JScrollPane;

public class StudyRoomSwingView extends JFrame implements StudyRoomView {

	private static final long serialVersionUID = 1L;

	private StudyRoomController studyRoomController;
	
	private JPanel contentPane;
	private JLabel lblId;
	private JTextField textFieldId;
	private JLabel lblName;
	private JTextField textFieldName;
	private JButton btnAdd;
	private JButton btnDelete;
	private JScrollPane scrollPane;
	private JList<User> listUsers;
	private JLabel lblErrorMessage;
	private JLabel lblErrorMessageFullRoom;
	
	private DefaultListModel<User> listUsersModel;


	/**
	 * Launch the application.
	 */
	/*
	 * public static void main(String[] args) { EventQueue.invokeLater(new
	 * Runnable() { public void run() { try { StudyRoomSwingView window = new
	 * StudyRoomSwingView(); window.setVisible(true); } catch (Exception e) {
	 * e.printStackTrace(); } } }); }
	 */
	
	DefaultListModel<User> getListUsersModel() {
		return listUsersModel;
	}

	/**
	 * Create the application.
	 */
	public StudyRoomSwingView() {
		
		KeyAdapter btnAddEnabler = new KeyAdapter() {
			@Override
			public void keyReleased(KeyEvent e) {
				if(lblErrorMessageFullRoom.getText().equals(" ")) {
					btnAdd.setEnabled(
							!textFieldId.getText().trim().isEmpty() &&
							!textFieldName.getText().trim().isEmpty()
						);
				}
			}
		};
		
		setTitle("StudyRoom View");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 450, 300);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWidths = new int[]{100, 100, 100, 100};
		gridBagLayout.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0};
		gridBagLayout.columnWeights = new double[]{1.0, 1.0, 1.0, 1.0};
		gridBagLayout.rowWeights = new double[]{0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		getContentPane().setLayout(gridBagLayout);
		
		lblId = new JLabel("id");
		lblId.setFont(new Font("Tahoma", Font.BOLD, 16));
		GridBagConstraints gbc_lblId= new GridBagConstraints();
		gbc_lblId.insets = new Insets(0, 0, 5, 5);
		gbc_lblId.gridx = 0;
		gbc_lblId.gridy = 0;
		getContentPane().add(lblId, gbc_lblId);
		
		textFieldId = new JTextField();
		textFieldId.setName("idTextBox");
		GridBagConstraints gbc_idTextField = new GridBagConstraints();
		gbc_idTextField.insets = new Insets(0, 0, 5, 5);
		gbc_idTextField.fill = GridBagConstraints.HORIZONTAL;
		gbc_idTextField.gridx = 1;
		gbc_idTextField.gridy = 0;
		gbc_idTextField.gridwidth = 3;
		getContentPane().add(textFieldId, gbc_idTextField);
		textFieldId.setColumns(10);
		textFieldId.addKeyListener(btnAddEnabler);

		lblName = new JLabel("name");
		lblName.setFont(new Font("Tahoma", Font.BOLD, 16));
		GridBagConstraints gbc_lblName = new GridBagConstraints();
		gbc_lblName.insets = new Insets(0, 0, 5, 5);
		gbc_lblName.gridx = 0;
		gbc_lblName.gridy = 1;
		contentPane.add(lblName, gbc_lblName);
		
		textFieldName = new JTextField();
		textFieldName.setName("nameTextBox");
		GridBagConstraints gbc_nameTextField = new GridBagConstraints();
		gbc_nameTextField.insets = new Insets(0, 0, 5, 5);
		gbc_nameTextField.fill = GridBagConstraints.HORIZONTAL;
		gbc_nameTextField.gridx = 1;
		gbc_nameTextField.gridy = 1;
		gbc_nameTextField.gridwidth = 3;
		contentPane.add(textFieldName, gbc_nameTextField);
		textFieldName.setColumns(10);
		textFieldName.addKeyListener(btnAddEnabler);
		
		btnAdd = new JButton("Add");
		btnAdd.setEnabled(false);
		GridBagConstraints gbc_btnAdd = new GridBagConstraints();
		//gbc_btnAdd.insets = new Insets(0, 0, 5, 5);
		gbc_btnAdd.gridx = 0;
		gbc_btnAdd.gridy = 2;
		gbc_btnAdd.gridwidth = 2;
		gbc_btnAdd.fill = GridBagConstraints.HORIZONTAL;
		contentPane.add(btnAdd, gbc_btnAdd);
		btnAdd.addActionListener(e -> studyRoomController.newUser(new User(textFieldId.getText(), textFieldName.getText())));
		
		btnDelete = new JButton("Delete");
		btnDelete.setEnabled(false);
		btnDelete.addActionListener(
				e -> studyRoomController.deleteUser(listUsers.getSelectedValue()));
		GridBagConstraints gbc_btnDelete = new GridBagConstraints();
		//gbc_btnDelete.insets = new Insets(0, 0, 5, 0);
		gbc_btnDelete.gridx = 2;
		gbc_btnDelete.gridy = 2;
		gbc_btnDelete.gridwidth = 2;
		gbc_btnDelete.fill = GridBagConstraints.HORIZONTAL;
		contentPane.add(btnDelete, gbc_btnDelete);
		
		scrollPane = new JScrollPane();
		GridBagConstraints gbc_scrollPane = new GridBagConstraints();
		gbc_scrollPane.insets = new Insets(0, 0, 5, 0);
		gbc_scrollPane.fill = GridBagConstraints.BOTH;
		gbc_scrollPane.gridwidth = 4;
		gbc_scrollPane.gridx = 0;
		gbc_scrollPane.gridy = 3;
		contentPane.add(scrollPane, gbc_scrollPane);
		
		listUsersModel = new DefaultListModel<>();
		listUsers = new JList<>(listUsersModel);
		listUsers.addListSelectionListener(e -> btnDelete.setEnabled(listUsers.getSelectedIndex() != -1));
		listUsers.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		listUsers.setName("usersList");
		scrollPane.setViewportView(listUsers);
		
		lblErrorMessageFullRoom = new JLabel(" ");
		lblErrorMessageFullRoom.setForeground(Color.RED);
		lblErrorMessageFullRoom.setName("errorMessageLabelFullRoom");
		GridBagConstraints gbc_lblErrorMessageFullRoom = new GridBagConstraints();
		gbc_lblErrorMessageFullRoom.gridwidth = 4;
		gbc_lblErrorMessageFullRoom.insets = new Insets(0, 0, 0, 5);
		gbc_lblErrorMessageFullRoom.gridx = 0;
		gbc_lblErrorMessageFullRoom.gridy = 4;
		contentPane.add(lblErrorMessageFullRoom, gbc_lblErrorMessageFullRoom);
		
		lblErrorMessage = new JLabel(" ");
		lblErrorMessage.setForeground(Color.RED);
		lblErrorMessage.setName("errorMessageLabel");
		GridBagConstraints gbc_lblErrorMessage = new GridBagConstraints();
		gbc_lblErrorMessage.gridwidth = 4;
		gbc_lblErrorMessage.insets = new Insets(0, 0, 0, 5);
		gbc_lblErrorMessage.gridx = 0;
		gbc_lblErrorMessage.gridy = 6;
		contentPane.add(lblErrorMessage, gbc_lblErrorMessage);
		
	}

	@Override
	public void userAdded(User user) {
		listUsersModel.addElement(user);
		resetErrorLabels();	}

	@Override
	public void showError(String message, User user) {
		lblErrorMessage.setText(message + ": " + user);		
	}

	@Override
	public void showError(String message) {
		lblErrorMessageFullRoom.setText(message);
		btnAdd.setEnabled(false);
		
	}

	@Override
	public void userRemoved(User user) {
		listUsersModel.removeElement(user);
		resetErrorLabels();		
	}

	public void setStudyRoomController(StudyRoomController studyRoomController) {
		this.studyRoomController = studyRoomController;
	}

	public Object showAllUsers(List<User> users) {
		users.stream().forEach(listUsersModel::addElement);
		return null;
	}
	
	private void resetErrorLabels() {
		lblErrorMessage.setText(" ");
		lblErrorMessageFullRoom.setText(" ");
	}
	
	

}