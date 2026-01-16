package com.app.colibri.view;

import static com.app.colibri.service.AppSettings.getLocaledItem;
import static com.app.colibri.service.MainLocaleManager.addTrackedItem;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Cursor;
import java.awt.EventQueue;
import java.awt.Font;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;

import com.app.colibri.controller.GUIController;
import com.app.colibri.controller.WordController;
import com.app.colibri.model.Box;
import com.app.colibri.model.User;
import com.app.colibri.service.AppRun;
import com.app.colibri.service.AppSettings;
import com.app.colibri.service.MainLocaleManager;
import com.app.colibri.service.crypt.CryptoException;
import com.app.colibri.service.crypt.CryptoUtils;
import com.app.colibri.service.crypt.Password;
import com.app.colibri.view.listeners.TextFieldClipboardMouseAdapter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Data;

@Data
public class LoginFrame {

	public static enum State {
		LOGIN, REGISTER;
	}

	private char echoChar;
	private String newUsernameBuffer = "";
	private String newUserPasswordBuffer = "";

	private JFrame frame;
	private State state;
	private JTextField tfLogin;
	private JPasswordField pfPassword;
	private JButton btnLogin;
	private JLabel lblRegister;
	private JLabel lblLocale;
	private JCheckBox chbAutoLogIn;
	private JCheckBox chbPwState;

	/**
	 * Launch the application.
	 */
	public static void launch(State state) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					LoginFrame window = new LoginFrame(state);
					window.frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the application.
	 */
	public LoginFrame() {
		initialize();
	}

	/**
	 * @wbp.parser.constructor
	 */
	public LoginFrame(State state) {
		this.state = state;
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		GUI.loginFrame = this;

		if (GUI.mainFrame != null) {
			GUI.mainFrame.setEnabled(false);
		}

		frame = new JFrame();
		frame.setResizable(false);
		changeTitle();
		final String imgPath = "images/colibri_icon.png";
		frame.setIconImage(Toolkit.getDefaultToolkit().getImage(MainFrame.class.getClassLoader().getResource(imgPath)));

		frame.addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosed(WindowEvent e) {
				GUI.loginFrame = null;
				if (GUI.mainFrame != null) {
					GUI.mainFrame.setEnabled(true);
					GUI.mainFrame.toFront();
				}
			}
		});
		frame.setBounds(100, 100, 300, 170);
		frame.setLocationRelativeTo(null);
		frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

		JPanel pnlMain = new JPanel();
		frame.getContentPane().add(pnlMain, BorderLayout.CENTER);
		pnlMain.setLayout(null);

		JLabel lblLogin = new JLabel("Username");
		lblLogin.setHorizontalAlignment(SwingConstants.RIGHT);
		addTrackedItem(lblLogin, "Username");
		lblLogin.setBounds(5, 27, 114, 16);
		pnlMain.add(lblLogin);

		tfLogin = new JTextField();
		tfLogin.addKeyListener(new KeyAdapter() {
			@Override
			public void keyReleased(KeyEvent e) {

				if (state == State.REGISTER) {
					final String newUsername = tfLogin.getText();

					if (!checkLoginPassConstraints(newUsername, 0)) {
						JOptionPane.showMessageDialog(null, getLocaledItem("username_constraint"), getLocaledItem("Warning"),
								JOptionPane.WARNING_MESSAGE);
						tfLogin.setText(newUsernameBuffer);
					} else {
						newUsernameBuffer = newUsername;
					}
				}

			}
		});
		tfLogin.addMouseListener(new TextFieldClipboardMouseAdapter(tfLogin, 2));
		tfLogin.setBounds(124, 25, 160, 20);
		pnlMain.add(tfLogin);
		tfLogin.setColumns(10);

		JLabel lblPassword = new JLabel("Password");
		lblPassword.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				chbPwState.setSelected(!chbPwState.isSelected());
				if (chbPwState.isSelected()) {
					pfPassword.setEchoChar((char) 0);
				} else {
					pfPassword.setEchoChar(echoChar);
				}
			}
		});
		lblPassword.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
		lblPassword.setHorizontalAlignment(SwingConstants.RIGHT);
		addTrackedItem(lblPassword, "Password", "show_hide_password");
		lblPassword.setBounds(42, 52, 77, 16);
		pnlMain.add(lblPassword);

		pfPassword = new JPasswordField();
		pfPassword.addKeyListener(new KeyAdapter() {
			@Override
			public void keyReleased(KeyEvent e) {

				if (state == State.REGISTER) {
					final String newPassword = new String(pfPassword.getPassword());

					if (!checkLoginPassConstraints(newPassword, 0)) {
						JOptionPane.showMessageDialog(null, getLocaledItem("pass_constraint"), getLocaledItem("Warning"),
								JOptionPane.WARNING_MESSAGE);
						pfPassword.setText(newUserPasswordBuffer);
					} else {
						newUserPasswordBuffer = newPassword;
					}
				}

			}
		});
		echoChar = pfPassword.getEchoChar();
		pfPassword.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseReleased(MouseEvent e) {
				if (e.getButton() == MouseEvent.BUTTON3) {
					String clip = GUIController.getFromClipboard();
					if (!clip.equals("")) {
						pfPassword.setText(clip);
					}
				} else if (e.getButton() == MouseEvent.BUTTON2) {
					pfPassword.setText("");
				}
			}
		});
		pfPassword.setBounds(124, 50, 160, 20);
		pnlMain.add(pfPassword);

		lblRegister = new JLabel("Register");
		lblRegister.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
		lblRegister.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				changeState();
			}
		});
		lblRegister.setHorizontalAlignment(SwingConstants.RIGHT);
		addTrackedItem(lblRegister, "Register");
		lblRegister.setBounds(42, 104, 77, 14);
		lblRegister.setToolTipText("");
		lblRegister.setForeground(Color.GRAY);
		lblRegister.setFont(new Font("Tahoma", Font.PLAIN, 11));
		pnlMain.add(lblRegister);

		btnLogin = new JButton("Log in");
		btnLogin.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {

				final String newUsername = tfLogin.getText();
				final String newPassword = new String(pfPassword.getPassword());

				File userFile = new File("UserData/" + newUsername + "/User.encrypted");
				ObjectMapper mapper = new ObjectMapper();

				if (state == State.LOGIN) {

					User user = AppSettings.appSettings.getUser();

					if (userFile.exists()) {

						try {
							String jsonUser = CryptoUtils.decrypt(AppSettings.KEY, userFile);
							user = mapper.readValue(jsonUser, User.class);
						} catch (IOException | CryptoException ex) {
							ex.printStackTrace();
						}

						if (user.getUserName().equals(newUsername)
								&& Password.checkPassword(newPassword, user.getUserPasswordHash())) {

							user.setAutoEnter(chbAutoLogIn.isSelected());
							AppSettings.appSettings.setUser(user);

							// Close mainFrame for reload it
							if (GUI.mainFrame != null) {
								GUI.mainFrame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
								GUI.mainFrame.dispose();
								GUI.mainFrame = null;
							}

							MainLocaleManager.removeAllItems();
							WordController.minRepeatTime = Long.MAX_VALUE;
							LoginFrame.appInit();
							frame.dispose();

						} else {
							JOptionPane.showMessageDialog(null, getLocaledItem("worong_name_or_pass"), getLocaledItem("Warning"),
									JOptionPane.WARNING_MESSAGE);
							return;
						}

					} else {
						JOptionPane.showMessageDialog(null, getLocaledItem("user_not_found"), getLocaledItem("Warning"),
								JOptionPane.WARNING_MESSAGE);
						return;
					}

				} else {

					if (userFile.exists()) {
						JOptionPane.showMessageDialog(null, getLocaledItem("user_exists"), getLocaledItem("Warning"),
								JOptionPane.WARNING_MESSAGE);
						return;
					} else if (!checkLoginPassConstraints(newUsername)) {
						JOptionPane.showMessageDialog(null, getLocaledItem("username_constraint"), getLocaledItem("Warning"),
								JOptionPane.WARNING_MESSAGE);
						return;
					} else if (!checkLoginPassConstraints(newPassword)) {
						JOptionPane.showMessageDialog(null, getLocaledItem("pass_constraint"), getLocaledItem("Warning"),
								JOptionPane.WARNING_MESSAGE);
						return;
					} else {

						User newUser = AppRun.appContext.getBean("user", User.class);
						newUser.setUserName(newUsername);
						newUser.setUserPasswordHash(Password.hashPassword(newPassword));

						try {
							File userFileDir = new File("UserData/" + newUsername);
							userFileDir.mkdir();

							String jsonNewUser = mapper.writeValueAsString(newUser);
							CryptoUtils.encrypt(AppSettings.KEY, jsonNewUser, userFile);

							if (GUI.mainFrame != null && WordController.allWordsList != null
									&& WordController.allWordsList.size() > 0) {
								final int answer = JOptionPane.showConfirmDialog(null,
										getLocaledItem("ask_copy_words_to_new_user"), getLocaledItem("Question"),
										JOptionPane.YES_NO_OPTION);

								if (answer == JOptionPane.YES_OPTION) {
									User currentUser = AppSettings.appSettings.getUser();
									AppSettings.appSettings.setUser(newUser);
									WordController.serializeAllWordsMain();
									AppSettings.appSettings.setUser(currentUser);
								}
							}

							changeState();
							JOptionPane.showMessageDialog(null, getLocaledItem("Registration success"), getLocaledItem("Info"),
									JOptionPane.INFORMATION_MESSAGE);
						} catch (CryptoException | JsonProcessingException ex) {
							ex.printStackTrace();
							JOptionPane.showMessageDialog(null, getLocaledItem("Registration error"), getLocaledItem("Error"),
									JOptionPane.ERROR_MESSAGE);
						}
					}

				}

			}
		});
		btnLogin.setMargin(new Insets(2, 2, 2, 2));
		addTrackedItem(btnLogin, "Log in");
		btnLogin.setBounds(199, 98, 85, 26);
		pnlMain.add(btnLogin);

		lblLocale = new JLabel("EN");
		lblLocale.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
		lblLocale.setFont(new Font("Tahoma", Font.PLAIN, 12));
		addTrackedItem(lblLocale, "EN");
		lblLocale.setBounds(4, 4, 30, 16);
		pnlMain.add(lblLocale);
		lblLocale.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				String lang = GUI.localeSwitchMap.get(lblLocale.getText());
				lblLocale.setText(lang);
				AppSettings.changeLocale(lang);
			}
		});
		lblLocale.setForeground(Color.GRAY);

		chbAutoLogIn = new JCheckBox("Auto log in");
		addTrackedItem(chbAutoLogIn, "Auto log in", "Auto log in");
		chbAutoLogIn.setBounds(199, 71, 85, 24);
		pnlMain.add(chbAutoLogIn);

		chbPwState = new JCheckBox("PWState");
		chbPwState.setVisible(false);
		chbPwState.setBounds(42, 72, 77, 24);
		pnlMain.add(chbPwState);

		MainLocaleManager.changeLocaleStatic();
	}

	private boolean checkLoginPassConstraints(String loginOrPass) {
		return checkLoginPassConstraints(loginOrPass, 3);
	}

	private boolean checkLoginPassConstraints(String loginOrPass, int min) {
		Pattern p = Pattern.compile("^[0-9a-zA-Zа-яА-ЯёЁ]{" + min + ",16}$");
		return p.matcher(loginOrPass).find();
	}

	private void changeTitle() {
		if (state == State.LOGIN) {
			addTrackedItem(frame, "Log in");
		} else {
			addTrackedItem(frame, "Register");
		}

		MainLocaleManager.changeLocaleStatic();
	}

	private void changeState() {

		String btnText;
		String lblText;

		if (state == State.LOGIN) {
			state = State.REGISTER;
			btnText = "Register";
			lblText = "Log in";
		} else {
			state = State.LOGIN;
			btnText = "Log in";
			lblText = "Register";
		}

		addTrackedItem(btnLogin, btnText);
		addTrackedItem(lblRegister, lblText);
		chbAutoLogIn.setVisible(state == State.LOGIN);

		changeTitle();
	}

	public static void appInit() {
		WordController.unserializeAllWordsMain();
		Box.refreshBox();
		SwingUtilities.invokeLater(GUI::new);
	}

}