package control;

import model.*;

import javax.swing.*;
import javax.swing.table.*;
import java.awt.*;
import java.util.*;
import java.util.regex.*;
import java.sql.*;
import java.security.MessageDigest;
import java.text.*;
import java.io.*;
import java.lang.Object;

/**
 * @author Sabba
 * 
 */
public class Control implements interfaces.I_Control {

	// JDBC driver name and database URL
	static final String DRIVER = "com.mysql.jdbc.Driver";
	static final String DATABASE_URL = "jdbc:mysql://localhost/MySQL";

	Connection connection = null; // manages connection
	Statement statement = null; // query statement
	ResultSet resultSet = null; // manages results
	public User user;

	public view.Message msg;

	public Control() {
		msg = new view.Message();
		initializeDatabase();
	}

	public String generatePassword(String password) {
		String algorithm = "SHA";
		byte[] plainText = password.getBytes();

		MessageDigest msgDig = null;
		try {
			msgDig = MessageDigest.getInstance(algorithm);
		} catch (Exception e) {
			e.printStackTrace();
		}

		msgDig.reset();
		msgDig.update(plainText);
		byte[] encodedPassword = msgDig.digest();

		StringBuffer encPassword = new StringBuffer();

		// convert some non-printable/non-readable characters into their
		// String representation to be read/stored
		for (int i = 0; i < encodedPassword.length; i++) {
			if ((encodedPassword[i] & 0xff) < 0x10) {
				encPassword.append("0");
			}

			encPassword.append(Long.toString(encodedPassword[i] & 0xff, 16));
		}

		return encPassword.toString();
	}

	public boolean addUser(JTextField usernameInput,
			JPasswordField passwordInput, JPasswordField password2Input,
			JComboBox typeInput, JTextField firstnameInput,
			JTextField surnameInput, JTextField dateOfBirthInput,
			JTextField nrInput) {
		String username = usernameInput.getText().trim();
		String password = new String(passwordInput.getPassword());
		String password2 = new String(password2Input.getPassword());
		String type = typeInput.getSelectedItem().toString().trim();
		String firstname = firstnameInput.getText().trim();
		String surname = surnameInput.getText().trim();
		String date = dateOfBirthInput.getText().trim();
		String tlf = nrInput.getText().trim();
		if (validateNewUser(username, password, password2, type, firstname,
				surname, date, tlf)) {
			// passed the validation and can be added to the DB
			String pass = new String(generatePassword(password));

			try {
				resultSet = statement
						.executeQuery("SELECT * from Users where Username = '"
								+ username + "'");
				if (resultSet.next()) {
					msg.messageDialog("Brukernavnet " + username
							+ " finnes allerede", 2);
					return false;
				}
			}// end try
			catch (SQLException sqlException) {
				msg.messageDialog("Kunne ikke legge inn ny bruker i databasen",
						0);
				initializeDatabase();
				return false;
			}
			// query database
			try {
				statement.executeUpdate("INSERT into Users values(" + "'"
						+ username + "', " + "'" + pass + "', " + "'"
						+ capitalizeFully(type) + "', " + "'"
						+ new User().getBackgroundColor().getRGB() + "', "
						+ "'" + new User().getForegroundColor().getRGB()
						+ "', " + "'" + new User().getFamily() + "', " + "'"
						+ new User().getStyle() + "', " + "'"
						+ new User().getSize() + "', " + "'"
						+ capitalizeFully(firstname) + "', " + "'"
						+ capitalizeFully(surname) + "', " + "'"
						+ sqlDateFormat(date) + "', " + "'" + tlf + "')");
			}// end try
			catch (SQLException sqlException) {
				msg.messageDialog("Kunne ikke legge inn ny bruker i databasen",
						0);
				initializeDatabase();
				return false;
			}
			msg.messageDialog("Brukeren " + username + " er lagt inn", 1);
			return true;
		} else {
			return false;
		}
	}

	public void closeDatabase() {
		try {
			if (resultSet != null) {
				resultSet.close();
			}
			statement.close();
			connection.close();
		}// end try
		catch (Exception exception) {
			msg.messageDialog("Databasen ble ikke lukket ordentlig", 0);
		}// end catch
	}// end closeDatabase()

	public void changePassword(User u, String oldText) {

		String[] result = msg.changePasswordDialog(oldText);
		if (result != null) {
			if (result[0].length() == 0 || result[1].length() == 0
					|| result[2].length() == 0) {
				msg.messageDialog("* Alle feltene m� fylles ut", 2);
			} else {
				// validate password
				if (result[0].length() < 8 | result[0].length() > 25) {
					msg.messageDialog(
							"* Passordet m� v�re mellom 8-25 tegn langt.", 2);
				} else if (specialCharacters(result[0], "password")) {
					msg.messageDialog(
							"* Passordet kan ikke inneholde spesialtegn eller mellomrom",
							2);
				} else if (!legalPassword(result[0])) {
					msg.messageDialog(
							"* Passordet m� inneholde minst et tall,\nen stor bokstav og en liten bokstav",
							2);
				} else if (!result[1].equals(result[0])) {
					// crosscheck the second passwordfield
					msg.messageDialog("* Passordfeltene m� v�re like", 2);
				} else if (!user.getPassword().equals(
						generatePassword(result[2]))) {
					if(oldText.contains("Admin"))
					msg.messageDialog("* Administratorpassordet er feil", 2);
					else
						msg.messageDialog("* Gammelt passord er feil", 2);
				} else {
					// all the criteria's are satisfied
					// change password
					u.setPassword(generatePassword(result[0]));
					if (updateUser(u)) {
						msg.messageDialog("Passordet er endret", 1);
					} else {
						msg.messageDialog("Passordet kunne ikke endres", 0);
					}
				}
			}
		}
	}

	public boolean deleteUser(String username) {
		// query database
		try {

			resultSet = statement
					.executeQuery("SELECT * FROM Users where Username = '"
							+ username + "'");
			if (resultSet.next()) {
				// found a match
				// confirm deleting
				String confirm = msg.singlePasswordDialog();
				if (confirm == null) {
					// cancelbutton pressed or inputField is empty
					return false;
				} else if (user.getPassword().equals(generatePassword(confirm))) {
					// we can remove the user account
					// but first check accountType
					// it's different reaction on the different accountType
					if (resultSet.getString("Type").equalsIgnoreCase(
							"Administrator")) {
						// AccountType equals "Administrator"
						try {
							resultSet = statement
									.executeQuery("SELECT count(*) from Users WHERE Type = 'Administrator'");
						}// end try
						catch (SQLException sqlException) {
							msg.messageDialog(
									"Kunne ikke koble opp mot databasen.", 0);
							initializeDatabase();
							return false;
						}// end catch

						if (resultSet.next() && resultSet.getInt(1) == 1) {
							try {

								statement
										.executeUpdate("INSERT into Users values "
												+ "('admin', "
												+ "'d033e22ae348aeb5660fc2140aec35850c4da997', "
												+ "'Administrator', "
												+ "'-16750849', "
												+ "'-1', "
												+ "'Verdana', "
												+ "'1', "
												+ "'14', "
												+ "'RKF', "
												+ "'Admin', "
												+ "'2005-12-17', "
												+ "'12345678')");

							}// end try
							catch (SQLException sqlException) {
								msg.messageDialog(
										"Kunne ikke koble opp mot databasen.",
										0);
								initializeDatabase();
								return false;
							}// end catch
						}
						try {
							statement
									.executeUpdate("DELETE FROM Users WHERE Username = '"
											+ username + "'");
						} // end try
						catch (SQLException sqlException) {
							msg.messageDialog("Kunne ikke slette brukeren "
									+ username, 0);
							initializeDatabase();
							return false;
						}// end catch

					} else {
						// AccountType equals "Standardbruker"
						try {
							statement
									.executeUpdate("DELETE FROM Users WHERE Username = '"
											+ username + "'");
						}// end try
						catch (SQLException sqlException) {
							msg.messageDialog("Kunne ikke slette brukeren "
									+ username + ".", 0);
							initializeDatabase();
							return false;
						}// end catch
					}
				} else {
					// wrong password
					msg.messageDialog("Feil passord", 2);
					return false;
				}
			} else {
				msg.messageDialog("Fant ikke brukeren", 0);
				return false;
			}
		}// end try
		catch (SQLException sqlException) {
			msg.messageDialog("Kunne ikke hente brukeren", 0);
			initializeDatabase();
			return false;
		}// end catch
		msg.messageDialog("Brukeren " + username + " er slettet", 1);
		return true;
	}

	public String[][] getUpdatePossibilities(String value, String field) {
		// get the correct field
		if (field.equals("Brukernavn"))
			field = "Username";
		else if (field.equals("Brukertype"))
			field = "Type";
		else if (field.equals("Fornavn"))
			field = "Firstname";
		else if (field.equals("Etternavn"))
			field = "Surname";
		else if (field.equals("F�dselsdato"))
			field = "DateOfBirth";
		else if (field.equals("Telefon"))
			field = "Phonenumber";

		// query database
		try {
			resultSet = statement.executeQuery("SELECT * FROM Users WHERE "
					+ field + " = '" + value + "'");
			if (field.equals("Username"))
				field = "Brukernavn";
			else if (field.equals("Type"))
				field = "Brukertype";
			else if (field.equals("Firstname"))
				field = "Fornavn";
			else if (field.equals("Surname"))
				field = "Etternavn";
			else if (field.equals("DateOfBirth"))
				field = "F�dselsdato";
			else if (field.equals("Phonenumber"))
				field = "Telefon";
			// process query results
			if (!resultSet.next()) {
				msg.messageDialog("Fant ingen bruker med\n" + field + " lik \""
						+ value + "\"", 1);
				return null;
			} else {
				resultSet.last();
				if (resultSet.getRow() == 1) {
					// only one result
					String[][] list = { {
							resultSet.getString("Username"),
							resultSet.getString("Type"),
							resultSet.getString("Firstname"),
							resultSet.getString("Surname"),
							outputDateFormat(resultSet.getDate("DateOfBirth")
									.toString()),
							resultSet.getString("Phonenumber") } };
					return list;
				} else {
					// more than one result

					int x = 0, y = 0;
					String[][] values = new String[resultSet.getRow()][6];
					resultSet.beforeFirst();
					while (resultSet.next()) {
						values[x][y++] = resultSet.getString("Username");
						values[x][y++] = resultSet.getString("Type");
						values[x][y++] = resultSet.getString("Firstname");
						values[x][y++] = resultSet.getString("Surname");
						values[x][y++] = outputDateFormat(resultSet.getDate(
								"DateOfBirth").toString());
						values[x][y++] = resultSet.getString("Phonenumber");

						x++;
						y = 0;
					}
					return values;
				}
			}
		}// end try
		catch (SQLException sqlException) {
			msg.messageDialog("Kunne ikke koble opp mot databasen", 0);
			initializeDatabase();
			return null;
		}// end catch
	}

	public User getUser(String name) {
		// query database
		try {
			resultSet = statement
					.executeQuery("SELECT * FROM Users where Username = '"
							+ name + "'");

			// process query results
			if (resultSet.next()) {
				User u = new User();
				u.setUsername(resultSet.getString("Username"));
				u.setPassword(resultSet.getString("Password"));
				u.setType(resultSet.getString("Type"));
				u.setBackgroundColor(resultSet.getInt("BackgroundC"));
				u.setForegroundColor(resultSet.getInt("ForegroundC"));
				u.setFont(resultSet.getString("FontType"),
						resultSet.getInt("FontStyle") - 1,
						resultSet.getInt("FontSize"));
				u.setFirstname(resultSet.getString("Firstname"));
				u.setSurname(resultSet.getString("Surname"));
				u.setdateofBirth(outputDateFormat(resultSet.getDate(
						"DateOfBirth").toString()));
				u.setPhonenumber(resultSet.getString("Phonenumber"));
				return u;
			}
		}// end try
		catch (SQLException sqlException) {
			msg.messageDialog("Fant ikke brukeren " + name, 0);
			initializeDatabase();
			return null;
		}// end catch
		return null;

	}

	public User[] getUsernameList() {
		model.User[] list;
		// query database
		try {
			resultSet = statement
					.executeQuery("SELECT * FROM Users ORDER BY Username");

			resultSet.last();
			list = new model.User[resultSet.getRow()];
			resultSet.beforeFirst();
			int i = 0;
			// process query results
			while (resultSet.next()) {
				model.User userlist = new User();

				userlist.setUsername(resultSet.getString("Username"));
				userlist.setPassword(resultSet.getString("Password"));
				userlist.setType(resultSet.getString("Type"));
				userlist.setBackgroundColor(resultSet.getInt("BackgroundC"));
				userlist.setForegroundColor(resultSet.getInt("ForegroundC"));
				userlist.setFont(resultSet.getString("FontType"),
						resultSet.getInt("FontStyle") - 1,
						resultSet.getInt("FontSize"));
				userlist.setFirstname(resultSet.getString("Firstname"));
				userlist.setSurname(resultSet.getString("Surname"));
				userlist.setdateofBirth(outputDateFormat(resultSet.getDate(
						"DateOfBirth").toString()));
				userlist.setPhonenumber(resultSet.getString("Phonenumber"));
				list[i++] = userlist;
			}
			return list;
		}// end try
		catch (SQLException sqlException) {
			msg.messageDialog("Kunne ikke hente en liste over brukerkontoer", 0);
			initializeDatabase();
			return null;
		}// end catch
	}

	public void initializeDatabase() {
		// connect to database MySQL and query database

		try {
			// run mysqld if not running
			if (!isRunning("mysqld.exe"))
				Runtime.getRuntime().exec("mysqld.exe");
			Thread.sleep(2000); // Sleep for 2sec to finish starting up
								// mysqld.exe

			// load the driver class
			Class.forName(DRIVER);

			// establish connection for querying to database
			connection = DriverManager.getConnection(DATABASE_URL, "root", "");

			// create Statement for querying database
			statement = connection.createStatement();
		}// end try
		catch (SQLException sqlException) {
			if (msg.confirmDialog("Kunne ikke koble til databasen.\nPr�ve igjen?") == 0) {
				initializeDatabase();
			} else {
				if (msg.confirmDialog("Avslutte programmet?") == 0)
					System.exit(0);
			}
		}// end catch
		catch (ClassNotFoundException classNotFound) {
			msg.messageDialog(
					"En teknisk feil gj�r at programmet ikke kan starte opp.\nKontakt systemansvarlig hvis feilen vedvarer",
					0);
			System.exit(0);
		}// end catch
		catch (IOException io) {
			io.printStackTrace();
		}// end catch
		catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	public boolean isRunning(String process) {
		boolean found = false;
		try {
			File file = File.createTempFile("realhowto", ".vbs");
			file.deleteOnExit();
			FileWriter fw = new java.io.FileWriter(file);

			String vbs = "Set WshShell = WScript.CreateObject(\"WScript.Shell\")\n"
					+ "Set locator = CreateObject(\"WbemScripting.SWbemLocator\")\n"
					+ "Set service = locator.ConnectServer()\n"
					+ "Set processes = service.ExecQuery _\n"
					+ " (\"select * from Win32_Process where name='"
					+ process
					+ "'\")\n"
					+ "For Each process in processes\n"
					+ "wscript.echo process.Name \n"
					+ "Next\n"
					+ "Set WSHShell = Nothing\n";

			fw.write(vbs);
			fw.close();
			Process p = Runtime.getRuntime().exec(
					"cscript //NoLogo " + file.getPath());
			BufferedReader input = new BufferedReader(new InputStreamReader(
					p.getInputStream()));
			String line;
			line = input.readLine();
			if (line != null) {
				if (line.equals(process)) {
					found = true;
				}
			}
			input.close();

		} // end try
		catch (Exception e) {
		} // end catch
		return found;
	}

	public boolean legalPassword(String password) {
		boolean number = false;
		boolean uppercase = false;
		boolean lowercase = false;
		for (int i = 0; i < password.length(); i++) {
			if (Character.isDigit(password.charAt(i))) {
				number = true;
			} else if (Character.isUpperCase(password.charAt(i))) {
				uppercase = true;
			} else if (Character.isLowerCase(password.charAt(i))) {
				lowercase = true;
			}
		}
		// we want at least one of all for the password to be valid
		return (number && uppercase && lowercase);
	}

	public void logOff() {

		Frame[] f = JFrame.getFrames();

		for (Frame frame : f) {
			frame.setEnabled(false);
			frame.dispose();
		}
		new view.Startup("logOff");
	}

	public String outputDateFormat(String sqlDate) {
		return new String(sqlDate.substring(8) + "-" + sqlDate.substring(5, 7)
				+ "-" + sqlDate.substring(0, 4)); // "31-01-2010"

	}

	public JTable searchUser(JTextField input, int index) {
		JComboBox box = new JComboBox();

		if (index == 0) {
			// username
			box.setModel(new DefaultComboBoxModel(
					new Object[] { "(Velg brukertype)" }));
			box.setSelectedIndex(0);
			return searchUser(input, box, new JTextField(), new JTextField(),
					new JTextField(), new JTextField());
		} else if (index == 1) {
			// accountType
			box.setModel(new DefaultComboBoxModel(new Object[] { input
					.getText().trim() }));
			box.setSelectedIndex(0);
			return searchUser(new JTextField(), box, new JTextField(),
					new JTextField(), new JTextField(), new JTextField());
		} else if (index == 2) {
			// Firstname
			box.setModel(new DefaultComboBoxModel(
					new Object[] { "(Velg brukertype)" }));
			box.setSelectedIndex(0);
			return searchUser(new JTextField(), box, input, new JTextField(),
					new JTextField(), new JTextField());
		} else if (index == 3) {
			// Surname
			box.setModel(new DefaultComboBoxModel(
					new Object[] { "(Velg brukertype)" }));
			box.setSelectedIndex(0);
			return searchUser(new JTextField(), box, new JTextField(), input,
					new JTextField(), new JTextField());
		} else if (index == 4) {
			// Date of Birth
			box.setModel(new DefaultComboBoxModel(
					new Object[] { "(Velg brukertype)" }));
			box.setSelectedIndex(0);
			return searchUser(new JTextField(), box, new JTextField(),
					new JTextField(), input, new JTextField());
		} else {
			// Phonenumber
			box.setModel(new DefaultComboBoxModel(
					new Object[] { "(Velg brukertype)" }));
			box.setSelectedIndex(0);
			return searchUser(new JTextField(), box, new JTextField(),
					new JTextField(), new JTextField(), input);
		}
	}

	public JTable searchUser(JTextField searchUsernameInput,
			JComboBox searchTypeInput, JTextField searchFirstnameInput,
			JTextField searchSurnameInput, JTextField searchDateOfBirthInput,
			JTextField searchNrInput) {
		String username = (searchUsernameInput.getText().trim()).toLowerCase();
		String type = capitalizeFully(searchTypeInput.getSelectedItem()
				.toString().trim());
		String firstname = capitalizeFully(searchFirstnameInput.getText()
				.trim());
		String surname = capitalizeFully(searchSurnameInput.getText().trim());
		String date = searchDateOfBirthInput.getText().trim();
		String tlf = searchNrInput.getText().trim();

		if (validateSearchUser(username, firstname, surname, date, tlf)) {
			// query database
			try {
				StringBuilder s = new StringBuilder();
				String[] condition = new String[7];
				int i = 0; // used for condition
				condition[i++] = "SELECT * from Users WHERE ";
				if (username.length() != 0)
					condition[i++] = "Username LIKE '%" + username + "%'";
				if (!type.equalsIgnoreCase("(Velg brukertype)"))
					condition[i++] = "Type = '" + type + "'";
				if (firstname.length() != 0)
					condition[i++] = "Firstname LIKE '%" + firstname + "%'";
				if (surname.length() != 0)
					condition[i++] = "Surname LIKE '%" + surname + "%'";
				if (date.length() != 0 && !date.equals("dd-mm-����")) {
					condition[i++] = "DateOfBirth = '" + sqlDateFormat(date)
							+ "'";
				}
				if (tlf.length() != 0)
					condition[i++] = "Phonenumber LIKE '%" + tlf + "%'";
				if (condition[1] != null) {
					s.append(condition[0]);
					s.append(condition[1]);
					for (i = 2; i < condition.length; i++) {
						if (condition[i] != null) {
							s.append(" OR ");
							s.append(condition[i]);
						}
					}
				} else {
					s.append("SELECT * from Users");
				}

				resultSet = statement.executeQuery(s.toString());

				// process query results
				if (!resultSet.next()) {
					msg.messageDialog("Fant ingen treff p� ditt s�k", 1);
					return null;
				} else {

					String[] columnName = { " Brukernavn ", " Brukertype ",
							" Fornavn ", " Etternavn ", " Telefon ",
							" F�dselsdato " };
					resultSet.last();
					String[][] values = new String[resultSet.getRow()][6];
					resultSet.beforeFirst();
					int x = 0, y = 0;

					while (resultSet.next()) {
						values[x][y++] = resultSet.getString("Username");
						values[x][y++] = resultSet.getString("Type");
						values[x][y++] = resultSet.getString("Firstname");
						values[x][y++] = resultSet.getString("Surname");
						values[x][y++] = resultSet.getString("Phonenumber");
						values[x][y++] = outputDateFormat(resultSet.getDate(
								"DateOfBirth").toString());
						x++;
						y = 0;
					}
					JTable result = new JTable(new DefaultTableModel(values,
							columnName)) {
						private static final long serialVersionUID = 1L;

						public boolean isCellEditable(int rowIndex, int colIndex) {
							return false; // Disallow the editing of any cell
						}
					};
					return result;
				}
			}// end try
			catch (SQLException sqlException) {
				msg.messageDialog("Kunne ikke fullf�re s�ket", 0);
				initializeDatabase();
				return null;
			}// end catch
		} else {
			return null;
		}

	}

	public void settings(String username) {
		user = getUser(username);
		view.Settings s = new view.Settings(user, this);
		s.setVisible(true);
	}

	public void shutdown() {
		closeDatabase();

		// close mysqld
		if (isRunning("mysqld.exe")) {
			try {
				Thread.sleep(3000);
				Runtime.getRuntime().exec("mysqld.exe").destroy();
			} catch (InterruptedException e) {
				e.printStackTrace();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		System.exit(0);
	}

	public boolean specialCharacters(String input, String type) {

		String regex = "[\\p{P}\\s+]"; // punctuation including space
		String Exc = "[-.\\s+]"; // exception for names, address and city
		String usernameExc = "[-_]"; // exception for username
		String tmp;
		Pattern pattern = Pattern.compile(regex);

		if (type.equals("name") | type.equals("address") | type.equals("city")
				| type.equals("comment")) {
			tmp = input.replaceAll(Exc, "");
		} else if (type.equals("username")) {
			tmp = input.replaceAll(usernameExc, "");
		} else {// passwordinput
			tmp = input;
		}

		return pattern.matcher(tmp).find();
	}

	public String sqlDateFormat(String inputDate) {
		if (inputDate.length() == 8)
			return new String(inputDate.substring(4, 8) + "-"
					+ inputDate.substring(2, 4) + "-"
					+ inputDate.substring(0, 2)); // "2010-01-31"
		else
			return new String(inputDate.substring(6, 10) + "-"
					+ inputDate.substring(3, 5) + "-"
					+ inputDate.substring(0, 2)); // "2010-01-31"
	}

	public String removeDateLine(String date) {
		return date.replaceAll("[^\\d]", "");
	}

	public boolean updateUser(model.User u) {

		if (validateUpdateUser(u.getType(), u.getFirstname(), u.getSurname(),
				removeDateLine(u.getdateofBirth()), u.getPhonenumber())) {

			// query database
			try {
				statement.executeUpdate("UPDATE Users " + "SET "
						+ "Password = '"
						+ u.getPassword()
						+ "', "
						+ "Type = '"
						+ u.getType()
						+ "', "
						+ "BackgroundC = '"
						+ u.getBackgroundColor().getRGB()
						+ "', "
						+ "ForegroundC = '"
						+ u.getForegroundColor().getRGB()
						+ "', "
						+ "FontType = '"
						+ u.getFont().getFamily()
						+ "', "
						+ "FontStyle = '"
						+ (u.getFont().getStyle() + 1)
						+ "', "
						+ "FontSize = '"
						+ u.getFont().getSize()
						+ "', "
						+ "Firstname = '"
						+ capitalizeFully(u.getFirstname())
						+ "', "
						+ "Surname = '"
						+ capitalizeFully(u.getSurname())
						+ "', "
						+ "DateOfBirth = '"
						+ sqlDateFormat(removeDateLine(u.getdateofBirth()))
						+ "', "
						+ "Phonenumber = '"
						+ u.getPhonenumber()
						+ "' "
						+ "WHERE Username = '"
						+ u.getUsername().toLowerCase()
						+ "'");
				return true;
			}// end try
			catch (SQLException sqlException) {
				initializeDatabase();
				return false;
			}
		}
		return false;
	}

	public boolean validateNewUser(String username, String password,
			String password2, String type, String firstname, String surname,
			String date, String tlf) {

		// validate username
		StringBuilder s = new StringBuilder();
		if (username.length() == 0) {
			s.append("* Brukernavn m� fylles ut\n");
		} else {// username is typed
			if (username.length() < 5 | username.length() > 20) {
				s.append("* Brukernavnet m� v�re mellom 5-20 tegn langt\n");
			} else if (specialCharacters(username, "username")) {
				s.append("* Brukernavnet kan ikke inneholde andre spesialtegn\n"
						+ "enn - og _ og tall\n");
			}
		}

		// validate password
		if (password.length() == 0) {
			s.append("* Passordet m� fylles ut\n");
		} else { // password is typed
			if (password.length() < 8 | password.length() > 25) {
				s.append("* Passordet m� v�re mellom 8-25 tegn langt\n");
			} else if (specialCharacters(password, "password")) {
				s.append("* Passordet kan ikke inneholde spesialtegn eller mellomrom\n");
			} else if (!legalPassword(password)) {
				s.append("* Passordet m� inneholde minst et tall,\nen stor bokstav og en liten bokstav\n");
			}
		}
		// crosscheck the second passwordfield
		if (!password2.equals(password)) {
			s.append("* Passordfeltene m� v�re like\n");
		}

		// validate firstname
		if (firstname.length() == 0) {
			s.append("* Fornavn m� fylles ut\n");
		} else {// firstname is typed
			if (validName(firstname) == 1) {
				s.append("* Fornavn kan bare inneholde\n A-�/a-�, -, ., og mellomrom\n");
			} else if (validName(firstname) == 2) {
				s.append("* Fornavn kan ikke slutte med -, .\n");
			}
		}

		// validate surname
		if (surname.length() == 0) {
			s.append("* Etternavn m� fylles ut\n");
		} else { // surname is typed
			if (firstname.equalsIgnoreCase(surname)) {
				s.append("* Fornavn og etternavn kan ikke v�re like");
			} else if (validName(surname) == 1) {
				s.append("* Etternavn kan bare inneholde\n A-�/a-�, -, og mellomrom\n");
			} else if (validName(firstname) == 2) {
				s.append("* Etternavn kan ikke slutte med -\n");
			}
		}

		// validate date of birth
		if (date.length() == 0) {
			s.append("* F�dselsdato m� fylles ut\n");
		} else { // date og birth is typed
			date = removeDateLine(date);
			if (validDate(date) == 1) {
				s.append("* F�dselsdato m� skrives slik: dd-mm-����\n");
			} else if (validDate(date) == 2) {
				s.append("* Ugyldig f�dselsdato\n");
			}
		}

		// validate phonenumber input
		if (tlf.length() == 0) {
			s.append("* Telefonnr m� fylles ut\n");
		} else { // phonenumber is typed
			if (tlf.length() != 8) {
				s.append("* Telefonnr kan bare inneholde 8 siffer\n");
			} else if (!validNr(tlf)) {
				s.append("* Ugyldig telefonnr\n");
			}
		}

		if (s.length() == 0) { // all input is OK
			return true;
		} else {
			msg.messageDialog(s.toString(), 2);
			return false;
		}
	}

	public boolean validateUpdateUser(String type, String firstname,
			String surname, String date, String tlf) {

		StringBuilder s = new StringBuilder();

		// validate firstname
		if (firstname.length() == 0) {
			s.append("* Fornavn m� fylles ut\n");
		} else {// firstname is typed
			if (validName(firstname) == 1) {
				s.append("* Fornavn kan bare inneholde\n A-�/a-�, -, ., og mellomrom\n");
			} else if (validName(firstname) == 2) {
				s.append("* Fornavn kan ikke slutte med - eller .\n");
			}
		}

		// validate surname
		if (surname.length() == 0) {
			s.append("* Etternavn m� fylles ut\n");
		} else { // surname is typed
			if (firstname.equalsIgnoreCase(surname)) {
				s.append("* Fornavn og etternavn kan ikke v�re like");
			} else if (validName(surname) == 1) {
				s.append("* Etternavn kan bare inneholde\n A-�/a-�, -, og mellomrom\n");
			} else if (validName(firstname) == 2) {
				s.append("* Etternavn kan ikke slutte med -\n");
			}
		}

		// validate date of birth
		if (date.length() == 0) {
			s.append("* F�dselsdato m� fylles ut\n");
		} else { // date og birth is typed
			date = removeDateLine(date);
			if (validDate(date) == 1) {
				s.append("* F�dselsdato m� skrives slik: dd-mm-����\n");
			} else if (validDate(date) == 2) {
				s.append("* Ugyldig f�dselsdato\n");
			}
		}

		// validate phonenumber input
		if (tlf.length() == 0) {
			s.append("* Telefonnr m� fylles ut\n");
		} else { // phonenumber is typed
			if (tlf.length() != 8) {
				s.append("* Telefonnr kan bare inneholde 8 siffer\n");
			} else if (!validNr(tlf)) {
				s.append("* Ugyldig telefonnr\n");
			}
		}

		if (s.length() == 0) { // all input is OK
			return true;
		} else {
			msg.messageDialog(s.toString(), 2);
			return false;
		}
	}

	public boolean validateSearchUser(String username, String firstname,
			String surname, String date, String tlf) {

		// validate username
		StringBuilder s = new StringBuilder();
		if (username.length() != 0) {// username is typed
			if (specialCharacters(username, username)) {
				s.append("* Brukernavnet kan ikke inneholde andre spesialtegn\n"
						+ "enn - og _ og tall");
			}
		}

		// validate firstname
		if (firstname.length() != 0) {// firstname is typed
			if (validName(firstname) == 1) {
				s.append("* Fornavn kan bare inneholde\n A-�/a-�, -, ., og mellomrom\n");
			} else if (validName(firstname) == 2) {
				s.append("* Fornavn kan ikke slutte med - eller .\n");
			}
		}

		// validate surname
		if (surname.length() != 0) { // surname is typed
			if (validName(surname) == 1) {
				s.append("* Etternavn kan bare inneholde\n A-�/a-�, -, og mellomrom\n");
			} else if (validName(surname) == 2) {
				s.append("* Etternavn kan ikke slutte med -\n");
			}
		}

		// validate date of birth
		if (date.length() != 0 && !date.equals("dd-mm-����")) { // date of birth
			date = removeDateLine(date);
			if (validDate(date) == 1) {
				s.append("* F�dselsdato m� skrives slik: dd-mm-����\n");
			} else if (validDate(date) == 2) {
				s.append("* Ugyldig f�dselsdato\n");
			}
		}

		// validate phonenumber input
		if (tlf.length() != 0) { // phonenumber is typed
			if (!validNr(tlf)) {
				s.append("* Ugyldig telefonnr\n");
			}
		}

		if (s.length() == 0) { // all input is OK
			return true;
		} else {
			msg.messageDialog(s.toString(), 2);
			return false;
		}
	}

	public User validateLogIn(JTextField username, JPasswordField password) {
		String pass = new String(password.getPassword());

		// query database
		try {
			resultSet = statement
					.executeQuery("SELECT * FROM Users where Username = '"
							+ username.getText() + "' AND Password = '"
							+ generatePassword(pass) + "'");

			// process query results
			if (!resultSet.next()) {
				// wrong username/password
				msg.messageDialog("Feil brukernavn/passord.", 0);
				return null;
			} else {

				user = new User();

				user.setUsername(resultSet.getString("Username"));
				user.setPassword(resultSet.getString("Password"));
				user.setType(resultSet.getString("Type"));
				user.setBackgroundColor(resultSet.getInt("BackgroundC"));
				user.setForegroundColor(resultSet.getInt("ForegroundC"));
				user.setFont(resultSet.getString("FontType"),
						resultSet.getInt("FontStyle") - 1,
						resultSet.getInt("FontSize"));
				user.setFirstname(resultSet.getString("Firstname"));
				user.setSurname(resultSet.getString("Surname"));
				user.setdateofBirth(outputDateFormat(resultSet.getDate(
						"DateOfBirth").toString()));
				user.setPhonenumber(resultSet.getString("Phonenumber"));

				new model.Manager(user);
				return user;
			}
		}// end try
		catch (SQLException sqlException) {
			msg.messageDialog("Fant ikke brukeren " + username, 0);
			initializeDatabase();
			return null;
		}// end catch
	}

	@SuppressWarnings("deprecation")
	public int validDate(String input) {
		Calendar calendar = Calendar.getInstance();
		SimpleDateFormat dateformat = new SimpleDateFormat("ddMMyyyy");
		try {
			dateformat.setLenient(false);
			dateformat.parse(input);
			if (dateformat.parse(input).after(calendar.getTime())
					| calendar.getTime().getYear()
							- dateformat.parse(input).getYear() > 100)
				return 2;
		} catch (ParseException e) {
			return 1;
		} catch (IllegalArgumentException e) {
			return 1;
		}

		return 0;

	}

	public int validName(String input) {
		for (int i = 0; i < input.length(); i++) {
			// check for digits
			if (Character.isDigit((int) input.charAt(i)))
				return 1;
		}
		if (!specialCharacters(input, "name")) {
			if (input.charAt(input.length() - 1) == '-') {
				return 2;
			}

		} else
			return 1;
		return 0;
	}

	public boolean validImageURL(String input) {
		return input.endsWith(".jpg") | input.endsWith(".jpeg")
				| input.endsWith(".jfif") | input.endsWith(".jfl")
				| input.endsWith(".gif") | input.endsWith(".bmp")
				| input.endsWith(".dib") | input.endsWith(".rle")
				| input.endsWith(".tif") | input.endsWith(".png")
				| input.endsWith(".wmf") | input.endsWith(".pcx")
				| input.endsWith(".JPG") | input.endsWith(".JPEG")
				| input.endsWith(".JFIF") | input.endsWith(".JFL")
				| input.endsWith(".GIF") | input.endsWith(".BMP")
				| input.endsWith(".DIB") | input.endsWith(".RLE")
				| input.endsWith(".TIF") | input.endsWith(".PNG")
				| input.endsWith(".WMF") | input.endsWith(".PCX");
	}

	public static String capitalizeFully(String str) {
		if (str == null || str.length() == 0) {
			return str;
		}
		str = str.toLowerCase();
		return capitalize(str);
	}

	public static String capitalize(String str) {
		if (str == null || str.length() == 0) {
			return str;
		}
		int strLen = str.length();
		StringBuffer buffer = new StringBuffer(strLen);
		boolean capitalizeNext = true;
		for (int i = 0; i < strLen; i++) {
			char ch = str.charAt(i);

			if (isDelimiter(ch)) {
				buffer.append(ch);
				capitalizeNext = true;
			} else if (capitalizeNext) {
				buffer.append(Character.toTitleCase(ch));
				capitalizeNext = false;
			} else {
				buffer.append(ch);
			}
		}
		return buffer.toString();
	}

	public static boolean isDelimiter(char ch) {
		return Character.isWhitespace(ch) | ch == '-';
	}

	public boolean validNr(String input) {
		for (int i = 0; i < input.length(); i++) {
			if (!Character.isDigit(input.charAt(i))) {
				return false;
			}
		}
		return true;
	}

	public boolean validEmail(String email) {
		String regex = "[_A-Za-z0-9-]+(\\.[_A-Za-z0-9-]+)*[A-Za-z0-9]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})";
		Pattern pattern = Pattern.compile(regex);
		return pattern.matcher(email).find();
	}

	public JTable getMainMemberLog() {
		String[][] result;
		int x = 0;
		JTable table;
		// query database
		try {
			resultSet = statement
					.executeQuery("SELECT * from MEMBERLOG ORDER BY DateTime DESC Limit 5");

			// process query results
			if (resultSet.next()) {
				resultSet.last();
				result = new String[resultSet.getRow()][4];
				resultSet.beforeFirst();

				while (resultSet.next()) {
					result[x][0] = " "
							+ getOutputDateTime(resultSet.getString("DateTime"));
					result[x][1] = " " + resultSet.getString("MemberID");
					result[x][2] = " " + resultSet.getString("Event");
					result[x][3] = " " + resultSet.getString("User");
					x++;
				}
				String[] columnName = { " Dato og tid ", " ID ",
						" Loggf�ring ", " Utf�rt av " };

				table = new JTable(new DefaultTableModel(result, columnName)) {
					private static final long serialVersionUID = 1L;

					public boolean isCellEditable(int rowIndex, int colIndex) {
						return false; // Disallow the editing of any cell
					}
				};
			} else {
				String[] columnName = { " " };
				result = new String[1][1];
				result[0][0] = "Ingen oppf�ringer tilgjengelig";
				table = new JTable(new DefaultTableModel(result, columnName)) {
					private static final long serialVersionUID = 1L;

					public boolean isCellEditable(int rowIndex, int colIndex) {
						return false; // Disallow the editing of any cell
					}
				};
			}
		} // end try
		catch (SQLException e) {
			String[] columnName = { " " };
			result = new String[1][1];
			result[0][0] = "Ingen oppf�ringer tilgjengelig";
			table = new JTable(new DefaultTableModel(result, columnName)) {
				private static final long serialVersionUID = 1L;

				public boolean isCellEditable(int rowIndex, int colIndex) {
					return false; // Disallow the editing of any cell
				}
			};
		}// end catch
		return table;
	}

	public JTable getMainActivityLog() {
		String[][] result;
		int x = 0;
		JTable table;

		// query database
		try {
			resultSet = statement
					.executeQuery("SELECT * from ACTIVITYLOG ORDER BY DateTime DESC Limit 5");

			// process query results
			if (resultSet.next()) {
				resultSet.last();
				result = new String[resultSet.getRow()][4];
				resultSet.beforeFirst();

				while (resultSet.next()) {
					result[x][0] = " "
							+ getOutputDateTime(resultSet.getString("DateTime"));
					result[x][1] = " " + resultSet.getString("ActivityID");
					result[x][2] = " " + resultSet.getString("Event");
					result[x][3] = " " + resultSet.getString("User");
					x++;
				}
				String[] columnName = { " Dato og tid ", " ID ",
						" Loggf�ring ", " Utf�rt av " };

				table = new JTable(new DefaultTableModel(result, columnName)) {
					private static final long serialVersionUID = 1L;

					public boolean isCellEditable(int rowIndex, int colIndex) {
						return false; // Disallow the editing of any cell
					}
				};
			} else {
				String[] columnName = { " " };
				result = new String[1][1];
				result[0][0] = "Ingen oppf�ringer tilgjengelig";
				table = new JTable(new DefaultTableModel(result, columnName)) {
					private static final long serialVersionUID = 1L;

					public boolean isCellEditable(int rowIndex, int colIndex) {
						return false; // Disallow the editing of any cell
					}
				};
			}
		} // end try
		catch (SQLException e) {
			String[] columnName = { " " };
			result = new String[1][1];
			result[0][0] = "Ingen oppf�ringer tilgjengelig";
			table = new JTable(new DefaultTableModel(result, columnName)) {
				private static final long serialVersionUID = 1L;

				public boolean isCellEditable(int rowIndex, int colIndex) {
					return false; // Disallow the editing of any cell
				}
			};
		} // / end catch
		return table;
	}

	public Member getMember(int memberID) {
		// query database
		try {
			Member m = new Member();
			String[][] result;
			String[][] result2;
			int x = 0;
			JTable table = null;
			JTable table2 = null;

			// query database
			resultSet = statement
					.executeQuery("SELECT * from MEMBER WHERE MemberID = '"
							+ memberID + "'");

			// process query results
			if (resultSet.next()) {
				m.setID(String.valueOf(memberID));
				m.setType(resultSet.getString("MembershipType"));
				m.setFirstname(resultSet.getString("Firstname"));
				m.setSurname(resultSet.getString("Surname"));
				m.setAddress(resultSet.getString("Address"));

				if (resultSet.getString("Zipcode").length() != 4) {
					String tmp = "0000" + resultSet.getString("Zipcode");
					m.setZipcode(tmp.substring(tmp.length() - 4, tmp.length()));
				} else
					m.setZipcode(resultSet.getString("Zipcode"));

				m.setDateOfBirth(outputDateFormat(resultSet
						.getString("DateOfBirth")));
				m.setGender(resultSet.getString("Gender"));
				m.setEnrollmentDate(outputDateFormat(resultSet
						.getString("EnrollmentDate")));
				m.setValidityDate(outputDateFormat(resultSet
						.getString("ValidityDate")));
				m.setTerminationDate(outputDateFormat(resultSet
						.getString("TerminationDate")));

				// query database
				resultSet = statement
						.executeQuery("SELECT City from City WHERE Zipcode = '"
								+ m.getZipcode() + "'");
				// process query results
				if (resultSet.next()) {
					m.setCity(resultSet.getString("City"));
				}

				// query database
				resultSet = statement
						.executeQuery("SELECT Email from EMAIL WHERE MemberID = '"
								+ m.getID() + "'");
				// process query results
				if (resultSet.next()) {
					m.setEmail(resultSet.getString("Email"));
				}

				// query database
				resultSet = statement
						.executeQuery("SELECT * from PHONENUMBER WHERE MemberID = '"
								+ m.getID() + "'");
				// process query results
				while (resultSet.next()) {
					if (resultSet.getString("Type").equalsIgnoreCase("Mobil")) {
						m.setMobilenr(resultSet.getString("Number"));
					} else if (resultSet.getString("Type").equalsIgnoreCase(
							"Privat")) {
						m.setPrivatenr(resultSet.getString("Number"));
					} else {
						m.setWorknr(resultSet.getString("Number"));
					}
				}

				// query database
				resultSet = statement
						.executeQuery("SELECT Image from IMAGE WHERE MemberID = '"
								+ m.getID() + "'");
				// process query results
				if (resultSet.next()) {
					m.setImage(resultSet.getString("Image"));
				}

				// query database
				resultSet = statement
						.executeQuery("SELECT * from MEMBERLOG WHERE MemberID = '"
								+ m.getID()
								+ "' ORDER BY DateTime DESC Limit 5");
				// process query results
				if (resultSet.next()) {
					resultSet.last();
					result = new String[resultSet.getRow()][4];
					resultSet.beforeFirst();

					while (resultSet.next()) {
						result[x][0] = getOutputDateTime(resultSet
								.getString("DateTime"));
						result[x][1] = resultSet.getString("MemberID");
						result[x][2] = resultSet.getString("Event");
						result[x][3] = resultSet.getString("User");
						x++;
					}
					String[] columnName = { " Dato og tid ", " ID ",
							" Loggf�ring ", " Utf�rt av " };

					table = new JTable(
							new DefaultTableModel(result, columnName)) {
						private static final long serialVersionUID = 1L;

						public boolean isCellEditable(int rowIndex, int colIndex) {
							return false; // Disallow the editing of any cell
						}
					};
				} else {
					String[] columnName = { " " };
					result = new String[1][1];
					result[0][0] = "Ingen oppf�ringer tilgjengelig";
					table = new JTable(
							new DefaultTableModel(result, columnName)) {
						private static final long serialVersionUID = 1L;

						public boolean isCellEditable(int rowIndex, int colIndex) {
							return false; // Disallow the editing of any cell
						}
					};
				}

				// query database
				resultSet = statement
						.executeQuery("SELECT * from MEMBERLOG WHERE MemberID = '"
								+ m.getID() + "' ORDER BY DateTime DESC");
				// process query results
				if (resultSet.next()) {
					resultSet.last();
					result2 = new String[resultSet.getRow()][4];
					resultSet.beforeFirst();
					x = 0;
					while (resultSet.next()) {
						result2[x][0] = getOutputDateTime(resultSet
								.getString("DateTime"));
						result2[x][1] = resultSet.getString("MemberID");
						result2[x][2] = resultSet.getString("Event");
						result2[x][3] = resultSet.getString("User");
						x++;
					}
					String[] columnName = { " Dato og tid ", " ID ",
							" Loggf�ring ", " Utf�rt av " };

					table2 = new JTable(new DefaultTableModel(result2,
							columnName)) {
						private static final long serialVersionUID = 1L;

						public boolean isCellEditable(int rowIndex, int colIndex) {
							return false; // Disallow the editing of any cell
						}
					};
				} else {
					String[] columnName = { " " };
					result2 = new String[1][1];
					result2[0][0] = "Ingen oppf�ringer tilgjengelig";
					table2 = new JTable(new DefaultTableModel(result2,
							columnName)) {
						private static final long serialVersionUID = 1L;

						public boolean isCellEditable(int rowIndex, int colIndex) {
							return false; // Disallow the editing of any cell
						}
					};
				}

			}
			m.setLog(table);
			m.setAllLog(table2);
			return m;
		} // end try
		catch (SQLException sqlException) {
			msg.messageDialog("Kunne ikke hente medlem " + memberID, 0);
			initializeDatabase();
			return null;
		}// end catch

	}

	public String getOutputDateTime(String sqlDateTime) {
		return sqlDateTime.substring(8, 10) // date
				+ "-" + sqlDateTime.substring(5, 7) // month
				+ "-" + sqlDateTime.substring(0, 4) // year
				+ " " + sqlDateTime.substring(11, 16); // time
	}

	public String getSqlDateTime(String inputDate) {
		return inputDate.substring(6, 10) + "-" + inputDate.substring(3, 5)
				+ "-" + inputDate.substring(0, 2) + " "
				+ inputDate.substring(11);
	}

	public String getCurrentDate() {
		return new SimpleDateFormat("dd-MM-yyyy")
				.format(Calendar.getInstance().getTime()).toString().trim();
	}

	public String getCurrentDateTime() {
		return new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
				.format(Calendar.getInstance().getTime()).toString().trim();
	}

	public void addMemberLog(String id, String event) {
		// query database
		try {
			statement.executeUpdate("INSERT INTO MEMBERLOG VALUES (" + "'" + id
					+ "', " + "'" + getSqlDateTime(getCurrentDateTime())
					+ "', " + "'" + this.user.getFirstname() + " "
					+ this.user.getSurname() + "', " + "'" + event + "')");
		}// end try
		catch (SQLException sqlException) {
			msg.messageDialog("Kunne ikke oppdatere medlemsloggen", 0);
			initializeDatabase();
		}// end catch
	}

	public Member terminateMembership(String id) {
		// query database
		try {
			statement.executeQuery("BEGIN");
			statement.executeUpdate("UPDATE MEMBER SET "
					+ "TerminationDate = '" + sqlDateFormat(getCurrentDate())
					+ "' " + "WHERE MemberID = '" + id + "'");
			addMemberLog(id, "Medlemsskapet termineres");
			statement.executeQuery("COMMIT");
			return getMember(Integer.parseInt(id));
		}// end try
		catch (SQLException sqlException) {
			msg.messageDialog("Kunne ikke terminere medlemsskapet", 0);
			initializeDatabase();
			return null;
		}// end catch
	}

	public boolean regContingent(String id, String date) {

		int year = Integer.parseInt(date.substring(6));
		if (year == Integer.parseInt(getCurrentDate().substring(6))) {
			date = date.substring(0, 6) + ++year;
			// query database
			try {
				statement.executeQuery("BEGIN");
				statement.executeUpdate("UPDATE MEMBER SET "
						+ "ValidityDate = '" + sqlDateFormat(date) + "', "
						+ "TerminationDate = '" + sqlDateFormat(date) + "' "
						+ "WHERE MemberID = '" + id + "'");
				addMemberLog(id, "Registrert innbetaling av medlemskontingent");
				statement.executeQuery("COMMIT");
				msg.messageDialog("Kontingenten er registrert"
						+ "\nNy gyldighetsdato er " + date, 1);
				return true;
			}// end try
			catch (SQLException sqlException) {
				msg.messageDialog("Kunne ikke registrere kontingent", 0);
				initializeDatabase();
				return false;
			}// end catch
		} else {
			msg.messageDialog(
					"Kontingenten for neste �r er allerede registrert", 2);
			return false;
		}
	}

	public Member updateMember(int id, String firstname, String surname,
			String addresse, String zipcode, String city, String date,
			String gender, String type, String email, String mobilNr,
			String privateNr, String workNr, String oldfirstname,
			String oldsurname, String oldaddresse, String oldzipcode,
			String oldcity, String olddate, String oldgender, String oldtype,
			String oldemail, String oldmobilNr, String oldprivateNr,
			String oldworkNr, String oldURL, String url) {

		date = removeDateLine(date);
		if (validateUpdateMember(firstname, surname, addresse, zipcode, city,
				date, gender, type, email, mobilNr, privateNr, workNr)) {
			// query database
			try {
				statement.executeUpdate("BEGIN");

				// add zipcode/city if !in table
				if (!(statement
						.executeQuery("SELECT * from CITY where Zipcode = '"
								+ zipcode + "'")).next()) {

					statement.executeUpdate("INSERT INTO CITY VALUES ('"
							+ zipcode + "', '" + capitalizeFully(city) + "')");
				}

				// update member
				statement.executeUpdate("UPDATE MEMBER SET "
						+ "MembershipType = '"
						+ type
						+ "', "
						+ "Firstname = '"
						+ capitalizeFully(firstname)
						+ "', "
						+ "Surname = '"
						+ capitalizeFully(surname)
						+ "', "
						+ "Address = '"
						+ capitalizeFully(addresse)
						+ "', "
						+ "Zipcode = '"
						+ zipcode
						+ "', "
						+ "DateOfBirth = '"
						+ sqlDateFormat(date)
						+ "', "
						+ "Gender = '"
						+ gender
						+ "' " + "WHERE MemberID = '" + id + "'");

				// update/add/delete email
				if (!oldemail.equalsIgnoreCase(email)) {
					if (oldemail.length() == 0) {
						statement.executeUpdate("INSERT INTO EMAIL VALUES ('"
								+ id + "', '" + email + "')");
					} else if (email.length() == 0) {
						statement
								.executeUpdate("DELETE FROM EMAIL WHERE MemberID = '"
										+ id + "'");
					} else {
						statement.executeUpdate("UPDATE EMAIL SET Email = '"
								+ email + "' WHERE MemberID = '" + id + "'");
					}
				}

				// update/add/delete phonenumbers

				// mobilenumber
				if (!oldmobilNr.equalsIgnoreCase(mobilNr)) {
					if (oldmobilNr.length() == 0) {
						statement
								.executeUpdate("INSERT INTO PHONENUMBER VALUES ('"
										+ id + "', 'Mobil', '" + mobilNr + "')");
					} else if (mobilNr.length() == 0) {
						statement
								.executeUpdate("DELETE FROM PHONENUMBER WHERE MemberID = '"
										+ id + "' AND Type = 'Mobil'");
					} else {
						statement
								.executeUpdate("UPDATE PHONENUMBER SET Number = '"
										+ mobilNr
										+ "' WHERE MemberID = '"
										+ id
										+ "' AND Type = 'Mobil'");
					}
				}

				// privatenumber
				if (!oldprivateNr.equalsIgnoreCase(privateNr)) {
					if (oldprivateNr.length() == 0) {
						statement
								.executeUpdate("INSERT INTO PHONENUMBER VALUES ('"
										+ id
										+ "', 'Privat', '"
										+ privateNr
										+ "')");
					} else if (privateNr.length() == 0) {
						statement
								.executeUpdate("DELETE FROM PHONENUMBER WHERE MemberID = '"
										+ id + "' AND Type = 'Privat'");
					} else {
						statement
								.executeUpdate("UPDATE PHONENUMBER SET Number = '"
										+ privateNr
										+ "' WHERE MemberID = '"
										+ id + "' AND Type = 'Privat'");
					}
				}

				// worknumber
				if (!oldworkNr.equalsIgnoreCase(workNr)) {
					if (oldworkNr.length() == 0) {
						statement
								.executeUpdate("INSERT INTO PHONENUMBER VALUES ('"
										+ id + "', 'Jobb', '" + workNr + "')");
					} else if (workNr.length() == 0) {
						statement
								.executeUpdate("DELETE FROM PHONENUMBER WHERE MemberID = '"
										+ id + "' AND Type = 'Jobb'");
					} else {
						statement
								.executeUpdate("UPDATE PHONENUMBER SET Number = '"
										+ workNr
										+ "' WHERE MemberID = '"
										+ id
										+ "' AND Type = 'Jobb'");
					}
				}

				// update/delete/add image
				if (!oldURL.equalsIgnoreCase(url)) {
					if (oldURL.equalsIgnoreCase("Image/Unknown.jpg")) {
						statement.executeUpdate("INSERT INTO IMAGE VALUES ("
								+ "'" + id + "', " + "'" + url + "')");
					} else if (url.equalsIgnoreCase("Image/Unknown.jpg")) {
						statement.executeUpdate("DELETE FROM IMAGE WHERE"
								+ " MemberID = '" + id + "'");
					} else {
						statement.executeUpdate("UPDATE IMAGE SET Image = '"
								+ url + "' WHERE MemberID = '" + id + "'");
					}
				}

				// add changes to memberlog
				if (!oldfirstname.equalsIgnoreCase(firstname)) {
					addMemberLog(Integer.toString(id), "Fornavn endret fra \""
							+ oldfirstname + "\" til \""
							+ capitalizeFully(firstname) + "\"");
				}

				if (!oldsurname.equalsIgnoreCase(surname)) {
					addMemberLog(Integer.toString(id),
							"Etternavn endret fra \"" + oldsurname
									+ "\" til \"" + capitalizeFully(surname)
									+ "\"");
				}

				if (!oldaddresse.equalsIgnoreCase(addresse)) {
					addMemberLog(Integer.toString(id), "Adresse endret fra \""
							+ oldaddresse + "\" til \""
							+ capitalizeFully(addresse) + "\"");
				}

				if (!oldzipcode.equals(zipcode)
						| !oldcity.equalsIgnoreCase(city)) {
					addMemberLog(Integer.toString(id), "Poststed endret fra \""
							+ oldzipcode + " " + capitalizeFully(oldcity)
							+ "\" til \"" + zipcode + " "
							+ capitalizeFully(city) + "\"");
				}

				if (!removeDateLine(olddate).equals(removeDateLine(date))) {
					date = removeDateLine(date);
					addMemberLog(
							Integer.toString(id),
							"F�dselsdato endret fra \"" + olddate + "\" til \""
									+ date.substring(0, 2) + "-"
									+ date.substring(2, 4) + "-"
									+ date.substring(4) + "\"");
				}

				if (!oldgender.equalsIgnoreCase(gender)) {
					addMemberLog(Integer.toString(id), "Kj�nn endret fra \""
							+ oldgender + "\" til \"" + gender + "\"");
				}

				if (!oldtype.equalsIgnoreCase(type)) {
					addMemberLog(Integer.toString(id),
							"Medlemsskapstype endret fra \"" + oldtype
									+ "\" til \"" + type + "\"");
				}

				if (!oldemail.equalsIgnoreCase(email)) {
					if (oldworkNr.length() == 0) {
						statement
								.executeUpdate("INSERT INTO PHONENUMBER VALUES ('"
										+ id + "', 'Jobb', '" + workNr + "')");
					} else if (workNr.length() == 0) {
						statement
								.executeUpdate("DELETE FROM PHONENUMBER WHERE MemberID = '"
										+ id + "' AND Type = 'Jobb'");
					} else {
						statement
								.executeUpdate("UPDATE PHONENUMBER SET Number = '"
										+ workNr
										+ "' WHERE MemberID = '"
										+ id
										+ "' AND Type = 'Jobb'");
					}
				}

				if (!oldmobilNr.equalsIgnoreCase(mobilNr)) {
					if (oldmobilNr.length() == 0) {
						addMemberLog(Integer.toString(id),
								"Mobilnummer er lagt til");
					} else if (mobilNr.length() == 0) {
						addMemberLog(Integer.toString(id),
								"Mobilnummer er fjernet");
					} else {
						addMemberLog(Integer.toString(id),
								"Mobilnummer er oppdatert");
					}
				}

				if (!oldprivateNr.equalsIgnoreCase(privateNr)) {
					if (oldprivateNr.length() == 0) {
						addMemberLog(Integer.toString(id),
								"Privatnummer er lagt til");
					} else if (privateNr.length() == 0) {
						addMemberLog(Integer.toString(id),
								"Privatnummer er fjernet");
					} else {
						addMemberLog(Integer.toString(id),
								"Privatnummer er oppdatert");
					}
				}

				if (!oldworkNr.equalsIgnoreCase(workNr)) {
					if (oldworkNr.length() == 0) {
						addMemberLog(Integer.toString(id),
								"Jobbnummer er lagt til");
					} else if (workNr.length() == 0) {
						addMemberLog(Integer.toString(id),
								"Jobbnummer er fjernet");
					} else {
						addMemberLog(Integer.toString(id),
								"Jobbnummer er oppdatert");
					}
				}

				if (!oldURL.equalsIgnoreCase(url)) {
					if (oldURL.equalsIgnoreCase("Image/Unknown.jpg")) {
						addMemberLog(Integer.toString(id),
								"Medlemsbilde er lagt til");
					} else if (url.equalsIgnoreCase("Image/Unknown.jpg")) {
						addMemberLog(Integer.toString(id),
								"Medlemsbilde er fjernet");
					} else {
						addMemberLog(Integer.toString(id),
								"Medlemsbilde er oppdatert");
					}
				}

				statement.executeQuery("COMMIT");
				return getMember(id);
			}// end try
			catch (SQLException sqlException) {
				msg.messageDialog("Kunne ikke oppdatere medlemmet", 0);
				initializeDatabase();
				return null;
			}// end catch
		}
		return null;
	}

	public boolean validateUpdateMember(String firstname, String surname,
			String addresse, String zipcode, String city, String date,
			String gender, String type, String email, String mobilNr,
			String privateNr, String workNr) {

		StringBuilder s = new StringBuilder();
		// validate firstname
		if (firstname.length() == 0) {
			s.append("* Fornavn m� fylles ut\n");
		} else if (validName(firstname) == 1) {
			s.append("* Fornavn kan bare inneholde\n A-�/a-�, -, ., og mellomrom\n");
		} else if (validName(firstname) == 2) {
			s.append("* Fornavn kan ikke slutte med - eller .\n");
		}

		// validate surname
		if (surname.length() == 0) {
			s.append("* Etternavn m� fylles ut\n");
		} else if (firstname.equalsIgnoreCase(surname)) {
			s.append("* Fornavn og etternavn kan ikke v�re like\n");
		} else if (validName(surname) == 1) {
			s.append("* Etternavn kan bare inneholde\n A-�/a-�, -, ., og mellomrom\n");
		} else if (validName(surname) == 2) {
			s.append("* Etternavn kan ikke slutte med - eller .\n");
		}

		// validate address
		if (addresse.length() == 0) {
			s.append("* Adressen m� fylles ut\n");
		} else if (specialCharacters(addresse, "address")) {
			s.append("* Adressen kan ikke inneholde spesialtegn\n");
		}

		// validate zipcode
		if (zipcode.length() == 0) {
			s.append("* Postnummer m� fylles ut\n");
		} else if (zipcode.length() != 4) {
			s.append("* Postnummer m� ha kun 4 siffer\n");
		} else if (!validNr(zipcode)) {
			s.append("* Ugyldig postnummer\n");
		}

		// validate city
		if (city.length() == 0) {
			s.append("* Poststed m� fylles ut\n");
		} else if (specialCharacters(city, "city")) {
			s.append("* Poststed kan ikke inneholde spesialtegn\n");
		} else {
			for (int i = 0; i < city.length(); i++) {
				if (Character.isDigit(city.charAt(i))) {
					s.append("* Poststed kan ikke inneholde tall\n");
					i = city.length();
				}
			}
		}

		// validate date of birth
		if (date.length() == 0) {
			s.append("* F�dselsdato m� fylles ut\n");
		} else { // date og birth is typed
			date = removeDateLine(date);
			if (validDate(date) == 1) {
				s.append("* F�dselsdato m� skrives slik: dd-mm-����\n");
			} else if (validDate(date) == 2) {
				s.append("* Ugyldig f�dselsdato\n");
			}
		}

		// validate email
		if (email.length() != 0) {
			if (!validEmail(email)) {
				s.append("* Epostadressen m� skrives slik\neksempel@domene.com");
			}
		}

		// validate mobilenr
		if (mobilNr.length() != 0) {
			if (mobilNr.length() != 8) {
				s.append("* Mobilnummer m� ha kun 8 siffer\n");
			} else if (!validNr(mobilNr)) {
				s.append("* Ugyldig mobilnummer\n");
			}
		}

		// validate privatenr
		if (privateNr.length() != 0) {
			if (privateNr.length() != 8) {
				s.append("* Privatnummer m� ha kun 8 siffer\n");
			} else if (!validNr(privateNr)) {
				s.append("* Ugyldig privatnummer\n");
			}
		}

		// validate worknr
		if (workNr.length() != 0) {
			if (workNr.length() != 8) {
				s.append("* Jobbnummer m� ha kun 8 siffer\n");
			} else if (!validNr(workNr)) {
				s.append("* Ugyldig jobbnummer\n");
			}
		}

		if (s.length() == 0)
			return true;
		else {
			msg.messageDialog(s.toString(), 2);
			return false;
		}
	}

	public boolean newMember(String firstname, String surname, String addresse,
			String zipcode, String city, String date, String gender,
			String type, String email, String mobilNr, String privateNr,
			String workNr, String url) {
		date = removeDateLine(date);
		if (validateNewMember(firstname, surname, addresse, zipcode, city,
				date, gender, type, email, mobilNr, privateNr, workNr)) {
			// query database
			try {
				statement.executeUpdate("BEGIN");

				int id = 1;
				resultSet = statement
						.executeQuery("SELECT max(MemberID) from MEMBER");
				if (resultSet.next()) {
					id = resultSet.getInt("max(MemberID)") + 1;
				}

				// add zipcode/city if !in table
				if (!(statement
						.executeQuery("SELECT * from CITY where Zipcode = '"
								+ zipcode + "'")).next()) {

					statement.executeUpdate("INSERT INTO CITY VALUES ('"
							+ zipcode + "', '" + capitalizeFully(city) + "')");
				}

				// add member
				String validitydate = getCurrentDate().substring(0, 6)
						+ Integer.toString((Integer.parseInt(getCurrentDate()
								.substring(6)) + 1));
				statement.executeUpdate("INSERT INTO MEMBER VALUES ('" + id
						+ "', '" + type + "', '" + capitalizeFully(firstname)
						+ "', '" + capitalizeFully(surname) + "', '"
						+ capitalizeFully(addresse) + "', '" + zipcode + "', '"
						+ sqlDateFormat(date) + "', '" + gender + "', '"
						+ sqlDateFormat(getCurrentDate()) + "', '"
						+ sqlDateFormat(validitydate) + "', '"
						+ sqlDateFormat(validitydate) + "')");

				// add email if exists
				if (email.length() != 0) {
					statement.executeUpdate("INSERT INTO EMAIL VALUES ('" + id
							+ "', '" + email + "')");
				}

				// add mobilenumber if exists
				if (mobilNr.length() != 0) {
					statement.executeUpdate("INSERT INTO PHONENUMBER VALUES ('"
							+ id + "', 'Mobil', '" + mobilNr + "')");
				}

				// add privatenumber if exists
				if (privateNr.length() != 0) {
					statement.executeUpdate("INSERT INTO PHONENUMBER VALUES ('"
							+ id + "', 'Privat', '" + privateNr + "')");
				}

				// add worknumber if exists
				if (workNr.length() != 0) {
					statement.executeUpdate("INSERT INTO PHONENUMBER VALUES ('"
							+ id + "', 'Jobb', '" + workNr + "')");
				}

				// add image if exists
				if (!url.equalsIgnoreCase("Image/Unknown.jpg")) {
					statement.executeUpdate("INSERT INTO IMAGE VALUES (" + "'"
							+ id + "', '" + url + "')");
				}

				addMemberLog(Integer.toString(id), "Opprettet medlemsskap");
				statement.executeQuery("COMMIT");
				return true;
			}// end try
			catch (SQLException sqlException) {
				msg.messageDialog("Kunne ikke opprette medlemsskapet", 0);
				initializeDatabase();
				return false;
			}// end catch
		}
		return false;
	}

	public boolean validateNewMember(String firstname, String surname,
			String addresse, String zipcode, String city, String date,
			String gender, String type, String email, String mobilNr,
			String privateNr, String workNr) {

		StringBuilder s = new StringBuilder();
		// validate firstname
		if (firstname.length() == 0) {
			s.append("* Fornavn m� fylles ut\n");
		} else if (validName(firstname) == 1) {
			s.append("* Fornavn kan bare inneholde\n A-�/a-�, -, ., og mellomrom\n");
		} else if (validName(firstname) == 2) {
			s.append("* Fornavn kan ikke slutte med -\n");
		}

		// validate surname
		if (surname.length() == 0) {
			s.append("* Etternavn m� fylles ut\n");
		} else if (firstname.equalsIgnoreCase(surname)) {
			s.append("* Fornavn og etternavn kan ikke v�re like\n");
		} else if (validName(surname) == 1) {
			s.append("* Etternavn kan bare inneholde\n A-�/a-�, -, ., og mellomrom\n");
		} else if (validName(surname) == 2) {
			s.append("* Etternavn kan ikke slutte med -\n");
		}

		// validate address
		if (addresse.length() == 0) {
			s.append("* Adressen m� fylles ut\n");
		} else if (specialCharacters(addresse, "address")) {
			s.append("* Adressen kan ikke inneholde spesialtegn\n");
		}

		// validate zipcode
		if (zipcode.length() == 0) {
			s.append("* Postnummer m� fylles ut\n");
		} else if (zipcode.length() != 4) {
			s.append("* Postnummer m� ha kun 4 siffer\n");
		} else if (!validNr(zipcode)) {
			s.append("* Ugyldig postnummer\n");
		}

		// validate city
		if (city.length() == 0) {
			s.append("* Poststed m� fylles ut\n");
		} else if (specialCharacters(city, "city")) {
			s.append("* Poststed kan ikke inneholde spesialtegn\n");
		} else {
			for (int i = 0; i < city.length(); i++) {
				if (Character.isDigit(city.charAt(i))) {
					s.append("* Poststed kan ikke inneholde tall\n");
					i = city.length();
				}
			}
		}

		// validate date of birth
		if (date.length() == 0 && !date.equals("dd-mm-����")) {
			s.append("* F�dselsdato m� fylles ut\n");
		} else { // date og birth is typed
			date = removeDateLine(date);
			if (validDate(date) == 1) {
				s.append("* F�dselsdato m� skrives slik: dd-mm-����\n");
			} else if (validDate(date) == 2) {
				s.append("* Ugyldig f�dselsdato\n");
			}
		}

		// validate gender
		if (gender.equalsIgnoreCase("( Velg )")) {
			s.append("* Kj�nn m� velges\n");
		}

		// validate membershiptype
		if (type.equalsIgnoreCase("( Velg )")) {
			s.append("* Medlemskapstype m� velges\n");
		}

		// validate email
		if (email.length() != 0) {
			if (!validEmail(email)) {
				s.append("* Epostadressen m� skrives slik\neksempel@domene.com");
			}
		}

		// validate mobilenr
		if (mobilNr.length() != 0) {
			if (mobilNr.length() != 8) {
				s.append("* Mobilnummer m� ha kun 8 siffer\n");
			} else if (!validNr(mobilNr)) {
				s.append("* Ugyldig mobilnummer\n");
			}
		}

		// validate privatenr
		if (privateNr.length() != 0) {
			if (privateNr.length() != 8) {
				s.append("* Privatnummer m� ha kun 8 siffer\n");
			} else if (!validNr(privateNr)) {
				s.append("* Ugyldig privatnummer\n");
			}
		}

		// validate worknr
		if (workNr.length() != 0) {
			if (workNr.length() != 8) {
				s.append("* Jobbnummer m� ha kun 8 siffer\n");
			} else if (!validNr(workNr)) {
				s.append("* Ugyldig jobbnummer\n");
			}
		}

		if (s.length() == 0)
			return true;
		else {
			msg.messageDialog(s.toString(), 2);
			return false;
		}
	}

	public JTable searchMember(String id, String firstname, String surname,
			String address, String zipcode, String city, String date,
			String gender, String type, String email, String nr, String status) {

		if (validateSearchMember(id, firstname, surname, address, zipcode,
				city, date, email, nr)) {
			// query database
			try {
				StringBuilder s = new StringBuilder();
				int emailCount = 0, nrCount = 0, cityCount = 0;
				int[] emailID = null, nrID = null, cityZipcode = null;
				int count = 0;

				if (email.length() != 0) {
					resultSet = statement
							.executeQuery("SELECT MemberID FROM EMAIL WHERE Email = '"
									+ email + "'");
					resultSet.last();
					emailCount = resultSet.getRow();
					resultSet.beforeFirst();
					emailID = new int[emailCount];
					while (resultSet.next()) {
						emailID[count++] = resultSet.getInt("MemberID");
					}

				}
				if (nr.length() != 0) {
					count = 0;
					resultSet = statement
							.executeQuery("SELECT MemberID FROM PHONENUMBER WHERE Number = '"
									+ nr + "'");
					resultSet.last();
					nrCount = resultSet.getRow();
					resultSet.beforeFirst();
					nrID = new int[nrCount];
					while (resultSet.next()) {
						nrID[count++] = resultSet.getInt("MemberID");
					}
				}

				if (city.length() != 0) {
					count = 0;
					resultSet = statement
							.executeQuery("SELECT Zipcode FROM CITY WHERE CITY.City LIKE '%"
									+ city + "%'");
					resultSet.last();
					cityCount = resultSet.getRow();
					resultSet.beforeFirst();
					cityZipcode = new int[cityCount];
					while (resultSet.next()) {
						cityZipcode[count++] = resultSet.getInt("Zipcode");
					}
				}

				String[] condition = new String[12 + emailCount + nrCount
						+ cityCount];
				int i = 0; // used for condition
				condition[i++] = "SELECT * from MEMBER left outer join CITY using (Zipcode) "
						+ "LEFT OUTER JOIN EMAIL using (MemberID) "
						+ "LEFT OUTER JOIN IMAGE using (MemberID) WHERE ";

				if (emailCount != 0) {
					s = new StringBuilder();
					s.append("MemberID IN (");
					int j = 0;
					for (; j < emailID.length - 1; j++) {
						s.append("'" + emailID[j] + "',");
					}
					s.append("'" + emailID[j] + "')");
					condition[i++] = s.toString();

				}

				if (nrCount != 0) {
					s = new StringBuilder();
					s.append("MemberID IN (");
					int j = 0;
					for (; j < nrID.length - 1; j++) {
						s.append("'" + nrID[j] + "',");
					}
					s.append("'" + nrID[j] + "')");
					condition[i++] = s.toString();
				}

				if (cityCount != 0) {
					s = new StringBuilder();
					s.append("Zipcode IN (");
					int j = 0;
					for (; j < cityZipcode.length - 1; j++) {
						s.append("'" + cityZipcode[j] + "',");
					}
					s.append("'" + cityZipcode[j] + "')");
					condition[i++] = s.toString();
				}

				if (id.length() != 0) {
					condition[i++] = "MemberID = '" + id + "'";
				}
				if (firstname.length() != 0) {
					condition[i++] = "Firstname LIKE '%" + firstname + "%'";
				}
				if (surname.length() != 0) {
					condition[i++] = "Surname LIKE '%" + surname + "%'";
				}
				if (address.length() != 0) {
					condition[i++] = "Address LIKE '%" + address + "%'";
				}
				if (zipcode.length() != 0) {
					condition[i++] = "Zipcode = '" + zipcode + "'";
				}
				if (date.length() != 0 && !date.equals("dd-mm-����")) {
					condition[i++] = "DateOfBirth LIKE '%"
							+ sqlDateFormat(removeDateLine(date)) + "%'";
				}
				if (!gender.equals("( Velg )")) {
					condition[i++] = "Gender = '" + gender + "'";
				}
				if (!type.equals("( Velg )")) {
					condition[i++] = "MembershipType = '" + type + "'";
				}
				if (status.equals("Aktiv")) {
					condition[i++] = "Terminationdate > '"
							+ sqlDateFormat(getCurrentDate()) + "'";
				} else if (status.equals("Inaktiv")) {
					condition[i++] = "Terminationdate < '"
							+ sqlDateFormat(getCurrentDate()) + "'";
				}

				s = new StringBuilder();

				if (condition[1] != null) {
					s.append(condition[0]);
					s.append(condition[1]);
					for (i = 2; i < condition.length; i++) {
						if (condition[i] != null) {
							s.append(" OR ");
							s.append(condition[i]);
						}
					}
				} else {
					s.append("SELECT * from MEMBER left outer join CITY using (Zipcode) "
							+ "LEFT OUTER JOIN EMAIL using (MemberID) "
							+ "LEFT OUTER JOIN IMAGE using (MemberID)");
				}

				resultSet = statement.executeQuery(s.toString());

				if (resultSet.next()) {
					resultSet.last();
					int row = resultSet.getRow();
					resultSet.beforeFirst();

					JTable table;
					String[] columnName = { " ID ", " Fornavn ", " Etternavn ",
							" Adresse ", " Postnr ", " Poststed ",
							" F�dselsdato ", " Kj�nn ", " Type ", " Status ",
							" Epost ", " Mobil ", " Privat ", " Jobb " };
					Object[][] values = new Object[row][14];
					for (int refresh = 0; refresh < row; refresh++) {
						values[refresh][13] = ""; // worknr
						values[refresh][12] = ""; // privatenr
						values[refresh][11] = ""; // mobilenr
						values[refresh][10] = ""; // email
					}

					row = 0;

					ResultSet numberSet = null;
					Statement statement2 = connection.createStatement();

					while (resultSet.next()) {
						numberSet = statement2
								.executeQuery("SELECT Type, Number from PHONENUMBER WHERE MemberID = '"
										+ resultSet.getString("MemberID") + "'");
						while (numberSet.next()) {
							if (numberSet.getString("Type").equals("Mobil")) {
								values[row][11] = " "
										+ numberSet.getString("Number");
							} else if (numberSet.getString("Type").equals(
									"Privat")) {
								values[row][12] = " "
										+ numberSet.getString("Number");
							} else if (numberSet.getString("Type").equals(
									"Jobb")) {
								values[row][13] = " "
										+ numberSet.getString("Number");
							}
						}

						values[row][0] = " " + resultSet.getString("MemberID");
						values[row][1] = " " + resultSet.getString("Firstname");
						values[row][2] = " " + resultSet.getString("Surname");
						values[row][3] = " " + resultSet.getString("Address");

						if (resultSet.getString("Zipcode").length() != 4) {
							String tmp = "0000"
									+ resultSet.getString("Zipcode");
							values[row][4] = " "
									+ tmp.substring(tmp.length() - 4,
											tmp.length());
						} else
							values[row][4] = " "
									+ resultSet.getString("Zipcode");

						values[row][5] = " " + resultSet.getString("City");
						values[row][6] = " "
								+ outputDateFormat(resultSet
										.getString("DateOfBirth"));
						values[row][7] = " " + resultSet.getString("Gender");
						values[row][8] = " "
								+ resultSet.getString("MembershipType");

						Calendar c = Calendar.getInstance();
						c.set(c.get(Calendar.YEAR), c.get(Calendar.MONTH),
								c.get(Calendar.DAY_OF_MONTH));

						Calendar d = Calendar.getInstance();
						d.set(Integer.parseInt(resultSet.getString(
								"TerminationDate").substring(0, 4)),
								Integer.parseInt(resultSet.getString(
										"TerminationDate").substring(5, 7)) - 1,
								Integer.parseInt(resultSet.getString(
										"TerminationDate").substring(8, 10)));

						values[row][9] = c.before(d) ? " Aktiv" : " Inaktiv";

						if (resultSet.getString("Email") != null) {
							values[row][10] = " "
									+ resultSet.getString("Email");
						}

						row++;
					}
					// atleast one result
					table = new JTable(
							new DefaultTableModel(values, columnName)) {
						private static final long serialVersionUID = 1L;

						public boolean isCellEditable(int rowIndex, int colIndex) {
							return false; // Disallow the editing of any cell
						}
					};

					return table;
				} else {
					// no result
					msg.messageDialog("Fant ingen treff p� ditt s�k", 2);
					return null;
				}

			} // end try
			catch (SQLException sqlException) {
				msg.messageDialog("Kunne ikke fullf�re s�ket", 0);
				initializeDatabase();
				return null;
			} // end catch
		}
		return null;
	}

	public boolean validateSearchMember(String id, String firstname,
			String surname, String address, String zipcode, String city,
			String date, String email, String nr) {

		StringBuilder s = new StringBuilder();

		// validate id
		if (id.length() != 0) {
			try {
				Integer.parseInt(id);
			} catch (Exception e) {
				s.append("* Medlemsnummer m� v�re et tall");
			}
		}

		// validate firstname
		if (firstname.length() != 0) {
			if (validName(firstname) == 1) {
				s.append("* Fornavn kan bare inneholde\n A-�/a-�, -, ., og mellomrom\n");
			} else if (validName(firstname) == 2) {
				s.append("* Fornavn kan ikke slutte med -\n");
			}
		}

		// validate surname
		if (surname.length() != 0) {
			if (validName(surname) == 1) {
				s.append("* Etternavn kan bare inneholde\n A-�/a-�, -, ., og mellomrom\n");
			} else if (validName(surname) == 2) {
				s.append("* Etternavn kan ikke slutte med -\n");
			}
		}

		// validate address
		if (specialCharacters(address, "address")) {
			s.append("* Adressen kan ikke inneholde spesialtegn\n");
		}

		// validate zipcode
		if (zipcode.length() != 0) {
			if (zipcode.length() != 4) {
				s.append("* Postnummer m� ha kun 4 siffer\n");
			} else if (!validNr(zipcode)) {
				s.append("* Ugyldig postnummer\n");
			}
		}

		// validate city
		if (city.length() != 0) {
			if (specialCharacters(city, "city")) {
				s.append("* Poststed kan ikke inneholde spesialtegn\n");
			} else {
				for (int i = 0; i < city.length(); i++) {
					if (Character.isDigit(city.charAt(i))) {
						s.append("* Poststed kan ikke inneholde tall\n");
						i = city.length();
					}
				}
			}
		}

		// validate date of birth
		if (date.length() != 0 && !date.equals("dd-mm-����")) {
			date = removeDateLine(date);
			if (validDate(date) == 1) {
				s.append("* F�dselsdato m� skrives slik: dd-mm-����\n");
			} else if (validDate(date) == 2) {
				s.append("* Ugyldig f�dselsdato\n");
			}
		}

		// validate email
		if (email.length() != 0) {
			if (!validEmail(email)) {
				s.append("* Epostadressen m� skrives slik\neksempel@domene.com\n");
			}
		}

		// validate nr
		if (nr.length() != 0) {
			if (nr.length() > 8) {
				s.append("* Nummer kan ikke ha mer enn 8 siffer\n");
			} else if (!validNr(nr)) {
				s.append("* Ugyldig nummer\n");
			}
		}

		if (s.length() == 0)
			return true;
		else {
			msg.messageDialog(s.toString(), 2);
			return false;
		}
	}

	public boolean validTime(String time) {
		return Pattern.matches("([01]?[0-9]|2[0-3]):[0-5][0-9]", time);
	}

	public void addActivityLog(String id, String event) {
		// query database
		try {
			statement.executeUpdate("INSERT INTO ACTIVITYLOG VALUES (" + "'"
					+ id + "', " + "'" + getSqlDateTime(getCurrentDateTime())
					+ "', " + "'" + this.user.getFirstname() + " "
					+ this.user.getSurname() + "', " + "'" + event + "')");
		}// end try
		catch (SQLException sqlException) {
			msg.messageDialog("Kunne ikke oppdatere aktivitetsloggen", 0);
			initializeDatabase();
		}// end catch
	}

	public Activity getActivity(String activityID) {
		// query database
		try {
			Activity a = new Activity();

			String[][] result;
			String[][] result2;
			int x = 0;
			JTable table = null;
			JTable table2 = null;

			resultSet = statement
					.executeQuery("SELECT * from ACTIVITY WHERE ActivityID = '"
							+ activityID + "'");
			if (resultSet.next()) {

				a.setID(activityID);
				a.setName(resultSet.getString("Name"));
				a.setAddress(resultSet.getString("Address"));

				if (resultSet.getString("Zipcode").length() != 4) {
					String tmp = "0000" + resultSet.getString("Zipcode");
					a.setZipcode(tmp.substring(tmp.length() - 4, tmp.length()));
				} else
					a.setZipcode(resultSet.getString("Zipcode"));

				a.setManager(resultSet.getString("Manager"));
				a.setDescription(resultSet.getString("Description"));
				a.setStartDate(outputDateFormat(resultSet
						.getString("StartDate")));
				a.setEndDate(outputDateFormat(resultSet.getString("EndDate")));
				a.setTotalSeats(resultSet.getString("TotalSeats"));

				resultSet = statement
						.executeQuery("SELECT City from City WHERE Zipcode = '"
								+ a.getZipcode() + "'");
				if (resultSet.next()) {

					a.setCity(resultSet.getString("City"));
				}

				resultSet = statement
						.executeQuery("SELECT Document FROM DOCUMENTS WHERE ActivityID = '"
								+ a.getID() + "'");
				if (resultSet.next()) {

					a.setDocument(resultSet.getString("Document"));
				}

				resultSet = statement
						.executeQuery("SELECT * FROM ACTIVITY_DAY WHERE ActivityID = '"
								+ a.getID() + "'");

				if (resultSet.next()) {
					Days[] days = a.getDays();
					resultSet.beforeFirst();
					while (resultSet.next()) {
						if (resultSet.getString("Day").equals("Alle dager")) {
							days[0] = new Days(resultSet.getString("Day"),
									resultSet.getString("StartTime").substring(
											0, 5), resultSet.getString(
											"EndTime").substring(0, 5));
						} else {
							if (resultSet.getString("Day").equals("Mandag")) {
								days[0] = new Days(resultSet.getString("Day"),
										resultSet.getString("StartTime")
												.substring(0, 5), resultSet
												.getString("EndTime")
												.substring(0, 5));
							} else if (resultSet.getString("Day").equals(
									"Tirsdag")) {
								days[1] = new Days(resultSet.getString("Day"),
										resultSet.getString("StartTime")
												.substring(0, 5), resultSet
												.getString("EndTime")
												.substring(0, 5));
							} else if (resultSet.getString("Day").equals(
									"Onsdag")) {
								days[2] = new Days(resultSet.getString("Day"),
										resultSet.getString("StartTime")
												.substring(0, 5), resultSet
												.getString("EndTime")
												.substring(0, 5));
							} else if (resultSet.getString("Day").equals(
									"Torsdag")) {
								days[3] = new Days(resultSet.getString("Day"),
										resultSet.getString("StartTime")
												.substring(0, 5), resultSet
												.getString("EndTime")
												.substring(0, 5));
							} else if (resultSet.getString("Day").equals(
									"Fredag")) {
								days[4] = new Days(resultSet.getString("Day"),
										resultSet.getString("StartTime")
												.substring(0, 5), resultSet
												.getString("EndTime")
												.substring(0, 5));
							} else if (resultSet.getString("Day").equals(
									"L�rdag")) {
								days[5] = new Days(resultSet.getString("Day"),
										resultSet.getString("StartTime")
												.substring(0, 5), resultSet
												.getString("EndTime")
												.substring(0, 5));
							} else if (resultSet.getString("Day").equals(
									"S�ndag")) {
								days[6] = new Days(resultSet.getString("Day"),
										resultSet.getString("StartTime")
												.substring(0, 5), resultSet
												.getString("EndTime")
												.substring(0, 5));
							}
						}
					}
					a.setDays(days);
				}

				resultSet = statement
						.executeQuery("SELECT * FROM ATTENDANCE WHERE ActivityID = '"
								+ a.getID() + "'");
				Attendance[] attendance = null;
				if (resultSet.next()) {

					resultSet.last();
					attendance = new Attendance[resultSet.getRow()];
					int att = 0;
					resultSet.beforeFirst();
					while (resultSet.next()) {
						attendance[att++] = new Attendance(
								resultSet.getString("ActivityID"),
								outputDateFormat(resultSet.getString("Date")),
								resultSet.getString("Total"),
								resultSet.getString("Comment"));
					}
					a.setAttendance(attendance);
				}

				resultSet = statement
						.executeQuery("SELECT * from ACTIVITYLOG WHERE ActivityID = '"
								+ a.getID()
								+ "' ORDER BY DateTime DESC Limit 5");

				if (resultSet.next()) {
					resultSet.last();
					result = new String[resultSet.getRow()][4];
					resultSet.beforeFirst();

					while (resultSet.next()) {
						result[x][0] = getOutputDateTime(resultSet
								.getString("DateTime"));
						result[x][1] = resultSet.getString("ActivityID");
						result[x][2] = resultSet.getString("Event");
						result[x][3] = resultSet.getString("User");
						x++;
					}
					String[] columnName = { " Dato og tid ", " ID ",
							" Loggf�ring ", " Utf�rt av " };

					table = new JTable(
							new DefaultTableModel(result, columnName)) {
						private static final long serialVersionUID = 1L;

						public boolean isCellEditable(int rowIndex, int colIndex) {
							return false; // Disallow the editing of any cell
						}
					};
				} else {
					String[] columnName = { " " };
					result = new String[1][1];
					result[0][0] = "Ingen oppf�ringer tilgjengelig";
					table = new JTable(
							new DefaultTableModel(result, columnName)) {
						private static final long serialVersionUID = 1L;

						public boolean isCellEditable(int rowIndex, int colIndex) {
							return false; // Disallow the editing of any cell
						}
					};
				}

				resultSet = statement
						.executeQuery("SELECT * from ACTIVITYLOG WHERE ActivityID = '"
								+ a.getID() + "' ORDER BY DateTime DESC");

				if (resultSet.next()) {
					resultSet.last();
					result2 = new String[resultSet.getRow()][4];
					resultSet.beforeFirst();
					x = 0;
					while (resultSet.next()) {
						result2[x][0] = getOutputDateTime(resultSet
								.getString("DateTime"));
						result2[x][1] = resultSet.getString("ActivityID");
						result2[x][2] = resultSet.getString("Event");
						result2[x][3] = resultSet.getString("User");
						x++;
					}
					String[] columnName = { " Dato og tid ", " ID ",
							" Loggf�ring ", " Utf�rt av " };

					table2 = new JTable(new DefaultTableModel(result2,
							columnName)) {
						private static final long serialVersionUID = 1L;

						public boolean isCellEditable(int rowIndex, int colIndex) {
							return false; // Disallow the editing of any cell
						}
					};
				} else {
					String[] columnName = { " " };
					result2 = new String[1][1];
					result2[0][0] = "Ingen oppf�ringer tilgjengelig";
					table2 = new JTable(new DefaultTableModel(result2,
							columnName)) {
						private static final long serialVersionUID = 1L;

						public boolean isCellEditable(int rowIndex, int colIndex) {
							return false; // Disallow the editing of any cell
						}
					};
				}

			}

			a.setLog(table);
			a.setAllLog(table2);
			return a;
		}// end try
		catch (SQLException sqlException) {
			msg.messageDialog("Kunne ikke hente aktivitet " + activityID, 0);
			initializeDatabase();
			return null;
		}// end catch
	}

	public boolean addActivity(String name, String manager, String adress,
			String zipcode, String city, String seats, String durationStart,
			String durationEnd, String allTF, String allTT, String mondayTF,
			String mondayTT, String tuesdayTF, String tuesdayTT,
			String wednesdayTF, String wednesdayTT, String thursdayTF,
			String thursdayTT, String fridayTF, String fridayTT,
			String saturdayTF, String saturdayTT, String sundayTF,
			String sundayTT, String description, String document) {

		if (validateAddActivity(name, manager, adress, zipcode, city, seats,
				durationStart, durationEnd, allTF, allTT, mondayTF, mondayTT,
				tuesdayTF, tuesdayTT, wednesdayTF, wednesdayTT, thursdayTF,
				thursdayTT, fridayTF, fridayTT, saturdayTF, saturdayTT,
				sundayTF, sundayTT)) {

			// query database
			try {
				statement.executeUpdate("BEGIN");
				int id = 1;
				resultSet = statement
						.executeQuery("SELECT max(ActivityID) from ACTIVITY");
				if (resultSet.next()) {
					id = resultSet.getInt("max(ActivityID)") + 1;
				}

				// add zipcode/city if !in table
				if (!(statement
						.executeQuery("SELECT * from CITY where Zipcode = '"
								+ zipcode + "'")).next()) {

					statement.executeUpdate("INSERT INTO CITY VALUES ('"
							+ zipcode + "', '" + capitalizeFully(city) + "')");
				}
				if (description.length() == 0)
					description = "Ingen kommentar tilgjengelig";
				statement.executeUpdate("INSERT INTO ACTIVITY VALUES(" + "'"
						+ id + "', " + "'" + name + "', " + "'"
						+ capitalizeFully(adress) + "', " + "'" + zipcode
						+ "', " + "'" + capitalizeFully(manager) + "', " + "'"
						+ description + "', " + "'"
						+ sqlDateFormat(removeDateLine(durationStart)) + "', "
						+ "'" + sqlDateFormat(removeDateLine(durationEnd))
						+ "', " + "'" + seats + "')");

				// add days
				if (allTF.length() != 0 && allTT.length() != 0) {
					statement.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
							+ "'" + id + "', " + "'Alle dager', " + "'" + allTF
							+ "', " + "'" + allTT + "')");
				} else {
					// find all days to insert
					if (mondayTF.length() != 0 && mondayTT.length() != 0) {
						statement
								.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
										+ "'"
										+ id
										+ "', "
										+ "'Mandag', "
										+ "'"
										+ mondayTF
										+ "', "
										+ "'"
										+ mondayTT
										+ "')");
					}
					if (tuesdayTF.length() != 0 && tuesdayTT.length() != 0) {
						statement
								.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
										+ "'"
										+ id
										+ "', "
										+ "'Tirsdag', "
										+ "'"
										+ tuesdayTF
										+ "', "
										+ "'"
										+ tuesdayTT + "')");
					}
					if (wednesdayTF.length() != 0 && wednesdayTT.length() != 0) {
						statement
								.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
										+ "'"
										+ id
										+ "', "
										+ "'Onsdag', "
										+ "'"
										+ wednesdayTF
										+ "', "
										+ "'"
										+ wednesdayTT + "')");
					}
					if (thursdayTF.length() != 0 && thursdayTT.length() != 0) {
						statement
								.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
										+ "'"
										+ id
										+ "', "
										+ "'Torsdag', "
										+ "'"
										+ thursdayTF
										+ "', "
										+ "'"
										+ thursdayTT + "')");
					}
					if (fridayTF.length() != 0 && fridayTT.length() != 0) {
						statement
								.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
										+ "'"
										+ id
										+ "', "
										+ "'Fredag', "
										+ "'"
										+ fridayTF
										+ "', "
										+ "'"
										+ fridayTT
										+ "')");
					}
					if (saturdayTF.length() != 0 && saturdayTT.length() != 0) {
						statement
								.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
										+ "'"
										+ id
										+ "', "
										+ "'L�rdag', "
										+ "'"
										+ saturdayTF
										+ "', "
										+ "'"
										+ saturdayTT
										+ "')");
					}
					if (sundayTF.length() != 0 && sundayTT.length() != 0) {
						statement
								.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
										+ "'"
										+ id
										+ "', "
										+ "'S�ndag', "
										+ "'"
										+ sundayTF
										+ "', "
										+ "'"
										+ sundayTT
										+ "')");
					}
				}

				// add document
				if (document.length() != 0) {
					statement.executeUpdate("INSERT INTO DOCUMENTS VALUES("
							+ "'" + id + "', " + "'" + document + "')");
				}

				// add event to activitylog
				addActivityLog(Integer.toString(id), "Opprettet aktivitet");

				statement.executeUpdate("COMMIT");
				msg.messageDialog("Aktviteten er registrert", 1);
				return true;
			}// end try
			catch (SQLException sqlException) {
				msg.messageDialog("Kunne ikke registrere aktiviteten", 0);
				initializeDatabase();
				return false;
			}// end catch
		}
		return false;
	}

	@SuppressWarnings("deprecation")
	public boolean validateAddActivity(String name, String manager,
			String adress, String zipcode, String city, String seats,
			String durationStart, String durationEnd, String allTF,
			String allTT, String mondayTF, String mondayTT, String tuesdayTF,
			String tuesdayTT, String wednesdayTF, String wednesdayTT,
			String thursdayTF, String thursdayTT, String fridayTF,
			String fridayTT, String saturdayTF, String saturdayTT,
			String sundayTF, String sundayTT) {

		StringBuilder s = new StringBuilder();
		// validate name
		if (name.length() == 0) {
			s.append("* Navn p� aktivitet p� fylles ut\n");
		} else if (validName(name) == 1) {
			s.append("* Navn p� aktivitet kan bare inneholde\n A-�/a-�, -, ., og mellomrom\n");
		} else if (validName(name) == 2) {
			s.append("* Navn p� aktivitet kan ikke slutte med -\n");
		}

		// validate manager
		if (manager.length() == 0) {
			s.append("* Navn p� ansvarshavende m� fylles ut\n");
		} else if (validName(manager) == 1) {
			s.append("* Navn p� ansvarshavende kan bare inneholde\n A-�/a-�, -, ., og mellomrom\n");
		} else if (validName(manager) == 2) {
			s.append("* Navn p� ansvarshavende kan ikke slutte med -\n");
		}

		// validate address
		if (adress.length() == 0) {
			s.append("* Adressen m� fylles ut\n");
		} else if (specialCharacters(adress, "address")) {
			s.append("* Adressen kan ikke inneholde spesialtegn\n");
		}

		// validate zipcode
		if (zipcode.length() == 0) {
			s.append("* Postnummer m� fylles ut\n");
		} else if (zipcode.length() != 4) {
			s.append("* Postnummer m� ha kun 4 siffer\n");
		} else if (!validNr(zipcode)) {
			s.append("* Ugyldig postnummer\n");
		}

		// validate city
		if (city.length() == 0) {
			s.append("* Poststed m� fylles ut\n");
		} else if (specialCharacters(city, "city")) {
			s.append("* Poststed kan ikke inneholde spesialtegn\n");
		} else {
			for (int i = 0; i < city.length(); i++) {
				if (Character.isDigit(city.charAt(i))) {
					s.append("* Poststed kan ikke inneholde tall\n");
					i = city.length();
				}
			}
		}

		// validate totalSeats
		if (seats.length() == 0) {
			s.append("* Antall plasser p� fylles ut\n");
		} else if (!validNr(seats)) {
			s.append("* Antall plasser m� v�re et heltall\n");
		}

		// validate duration dates
		if ((durationStart.length() == 0 | durationStart.equals("dd-mm-����"))
				| (durationEnd.length() == 0 | durationEnd.equals("dd-mm-����"))) {
			s.append("* Begge datofeltene m� fylles ut\n");
		} else {
			boolean okDate = true;
			// validate startdate
			durationStart = removeDateLine(durationStart);
			try {
				SimpleDateFormat dateformat = new SimpleDateFormat("ddMMyyyy");
				dateformat.setLenient(false);
				dateformat.parse(durationStart);

			} catch (Exception e) {
				okDate = false;
				s.append("* Startdato m� skrives p� f�lgende format: dd-mm-����\n");
			}

			// validate enddate
			if (okDate) {
				durationEnd = removeDateLine(durationEnd);
				try {
					SimpleDateFormat dateformat = new SimpleDateFormat(
							"ddMMyyyy");
					dateformat.setLenient(false);
					dateformat.parse(durationEnd);

					if (dateformat.parse(durationEnd).before(
							dateformat.parse(durationStart))) {
						s.append("* Sluttdato kan ikke v�re f�r startdato\n");
					}
				} catch (Exception e) {
					s.append("* Sluttdato m� skrives p� f�lgende format: dd-mm-����\n");
				}

			}

			if (okDate) {
				if (Integer.parseInt(durationStart.substring(4)) < 1900 + Calendar
						.getInstance().getTime().getYear() - 1
						| Integer.parseInt(durationStart.substring(4)) > 1900 + Calendar
								.getInstance().getTime().getYear() + 1) {
					s.append("* Startdato m� v�re innenfor �rene "
							+ (1900 + Calendar.getInstance().getTime()
									.getYear() - 1)
							+ " - "
							+ (1900 + Calendar.getInstance().getTime()
									.getYear() + 1) + "\n");
				}
			}

		}

		// validate time
		// non of the fields have input
		if (allTF.length() == 0 && allTT.length() == 0
				&& mondayTF.length() == 0 && mondayTT.length() == 0
				&& tuesdayTF.length() == 0 && tuesdayTT.length() == 0
				&& wednesdayTF.length() == 0 && wednesdayTT.length() == 0
				&& thursdayTF.length() == 0 && thursdayTT.length() == 0
				&& fridayTF.length() == 0 && fridayTT.length() == 0
				&& saturdayTF.length() == 0 && saturdayTT.length() == 0
				&& sundayTF.length() == 0 && sundayTT.length() == 0) {
			s.append("* Tidspunktet m� fylles ut");
		} else {
			if ((allTF.length() != 0 && allTT.length() == 0)
					| (allTF.length() == 0 && allTT.length() != 0)) {
				s.append("* Begge feltene i tidspunktet for \"alle dager\" m� fylles ut\n");
			} else if (allTF.length() != 0 && allTT.length() != 0) {
				if (!validTime(allTF) | !validTime(allTT)) {
					s.append("* Tidspunktet for \"alle dager\" er ugyldig\nKorrekt format er tt:mm\n");
				} else {

					try {
						if (new SimpleDateFormat("HH:mm").parse(allTF).after(
								new SimpleDateFormat("HH:mm").parse(allTT)))
							s.append("* Starttiden for \"alle dager\" m� v�re f�r sluttiden\n");
					} catch (ParseException e) {
						// Invalid date was entered
					}
				}
			} else {
				// all doesn't have input

				// check monday
				if ((mondayTF.length() != 0 && mondayTT.length() == 0)
						| (mondayTF.length() == 0 && mondayTT.length() != 0)) {
					s.append("* Begge feltene i tidspunktet for mandag m� fylles ut\n");
				} else if (mondayTF.length() != 0 && mondayTT.length() != 0) {
					if (!validTime(mondayTF) | !validTime(mondayTT)) {
						s.append("* Tidspunktet for mandag er ugyldig\nKorrekt format er tt:mm\n");
					} else {

						try {
							if (new SimpleDateFormat("HH:mm").parse(mondayTF)
									.after(new SimpleDateFormat("HH:mm")
											.parse(mondayTT)))
								s.append("* Starttiden for mandag m� v�re f�r sluttiden\n");
						} catch (ParseException e) {
							// Invalid date was entered
						}
					}
				}

				// check tuesday
				if ((tuesdayTF.length() != 0 && tuesdayTT.length() == 0)
						| (tuesdayTF.length() == 0 && tuesdayTT.length() != 0)) {
					s.append("* Begge feltene i tidspunktet for tirsdag m� fylles ut\n");
				} else if (tuesdayTF.length() != 0 && tuesdayTT.length() != 0) {
					if (!validTime(tuesdayTF) | !validTime(tuesdayTT)) {
						s.append("* Tidspunktet for tirsdag er ugyldig\nKorrekt format er tt:mm\n");
					} else {

						try {
							if (new SimpleDateFormat("HH:mm").parse(tuesdayTF)
									.after(new SimpleDateFormat("HH:mm")
											.parse(tuesdayTT)))
								s.append("* Starttiden for tirsdag m� v�re f�r sluttiden\n");
						} catch (ParseException e) {
							// Invalid date was entered
						}
					}
				}

				// check wednesday
				if ((wednesdayTF.length() != 0 && wednesdayTT.length() == 0)
						| (wednesdayTF.length() == 0 && wednesdayTT.length() != 0)) {
					s.append("* Begge feltene i tidspunktet for onsdag m� fylles ut\n");
				} else if (wednesdayTF.length() != 0
						&& wednesdayTT.length() != 0) {
					if (!validTime(wednesdayTF) | !validTime(wednesdayTT)) {
						s.append("* Tidspunktet for onsdag er ugyldig\nKorrekt format er tt:mm\n");
					} else {

						try {
							if (new SimpleDateFormat("HH:mm")
									.parse(wednesdayTF).after(
											new SimpleDateFormat("HH:mm")
													.parse(wednesdayTT)))
								s.append("* Starttiden for onsdag m� v�re f�r sluttiden\n");
						} catch (ParseException e) {
							// Invalid date was entered
						}
					}
				}

				// check thursday
				if ((thursdayTF.length() != 0 && thursdayTT.length() == 0)
						| (thursdayTF.length() == 0 && thursdayTT.length() != 0)) {
					s.append("* Begge feltene i tidspunktet for torsdag m� fylles ut\n");
				} else if (thursdayTF.length() != 0 && thursdayTT.length() != 0) {
					if (!validTime(thursdayTF) | !validTime(thursdayTT)) {
						s.append("* Tidspunktet for torsdag er ugyldig\nKorrekt format er tt:mm\n");
					} else {

						try {
							if (new SimpleDateFormat("HH:mm").parse(thursdayTF)
									.after(new SimpleDateFormat("HH:mm")
											.parse(thursdayTT)))
								s.append("* Starttiden for torsdag m� v�re f�r sluttiden\n");
						} catch (ParseException e) {
							// Invalid date was entered
						}
					}
				}

				// check friday
				if ((fridayTF.length() != 0 && fridayTT.length() == 0)
						| (fridayTF.length() == 0 && fridayTT.length() != 0)) {
					s.append("* Begge feltene i tidspunktet for fredag m� fylles ut\n");
				} else if (fridayTF.length() != 0 && fridayTT.length() != 0) {
					if (!validTime(fridayTF) | !validTime(fridayTT)) {
						s.append("* Tidspunktet for fredag er ugyldig\nKorrekt format er tt:mm\n");
					} else {

						try {
							if (new SimpleDateFormat("HH:mm").parse(fridayTF)
									.after(new SimpleDateFormat("HH:mm")
											.parse(fridayTT)))
								s.append("* Starttiden for fredag m� v�re f�r sluttiden\n");
						} catch (ParseException e) {
							// Invalid date was entered
						}
					}
				}

				// check saturday
				if ((saturdayTF.length() != 0 && saturdayTT.length() == 0)
						| (saturdayTF.length() == 0 && saturdayTT.length() != 0)) {
					s.append("* Begge feltene i tidspunktet for l�rdag m� fylles ut\n");
				} else if (saturdayTF.length() != 0 && saturdayTT.length() != 0) {
					if (!validTime(saturdayTF) | !validTime(saturdayTT)) {
						s.append("* Tidspunktet for l�rdag er ugyldig\nKorrekt format er tt:mm\n");
					} else {

						try {
							if (new SimpleDateFormat("HH:mm").parse(saturdayTF)
									.after(new SimpleDateFormat("HH:mm")
											.parse(saturdayTT)))
								s.append("* Starttiden for l�rdag m� v�re f�r sluttiden\n");
						} catch (ParseException e) {
							// Invalid date was entered
						}
					}
				}

				// check sunday
				if ((sundayTF.length() != 0 && sundayTT.length() == 0)
						| (sundayTF.length() == 0 && sundayTT.length() != 0)) {
					s.append("* Begge feltene i tidspunktet for s�ndag m� fylles ut\n");
				} else if (sundayTF.length() != 0 && sundayTT.length() != 0) {
					if (!validTime(sundayTF) | !validTime(sundayTT)) {
						s.append("* Tidspunktet for s�ndag er ugyldig\nKorrekt format er tt:mm\n");
					} else {

						try {
							if (new SimpleDateFormat("HH:mm").parse(sundayTF)
									.after(new SimpleDateFormat("HH:mm")
											.parse(sundayTT)))
								s.append("* Starttiden for s�ndag m� v�re f�r sluttiden\n");
						} catch (ParseException e) {
							// Invalid date was entered
						}
					}
				}
			}
		}

		if (s.length() == 0)
			return true;
		else {
			msg.messageDialog(s.toString(), 2);
			return false;
		}

	}

	public JTable searchActivity(String name, String manager, String adr,

	String zipcode, String city, String seats, String status,
			String durationStart, String durationEnd, String doc,
			JCheckBox all, JCheckBox monday, JCheckBox tuesday,
			JCheckBox wednesday, JCheckBox thursday, JCheckBox friday,
			JCheckBox saturday, JCheckBox sunday, FontMetrics fontmetrics) {

		if (validateSearchActivity(name, manager, adr, zipcode, city, seats,
				durationStart, durationEnd, doc)) {
			// query database
			try {
				StringBuilder s = new StringBuilder();

				int dayCount = 0, cityCount = 0;
				int[] dayID = null, cityZipcode = null;
				int count = 0;

				if (all.isSelected()) {
					resultSet = statement
							.executeQuery("SELECT DISTINCT ActivityID FROM ACTIVITY_DAY");
					resultSet.last();
					dayCount = resultSet.getRow();
					dayID = new int[dayCount];
					resultSet.beforeFirst();
					while (resultSet.next()) {
						dayID[count++] = resultSet.getInt("ActivityID");
					}

				} else if (monday.isSelected() | tuesday.isSelected()
						| wednesday.isSelected() | thursday.isSelected()
						| friday.isSelected() | saturday.isSelected()
						| sunday.isSelected()) {
					count = 0;
					if (monday.isSelected())
						count++;
					if (tuesday.isSelected())
						count++;
					if (wednesday.isSelected())
						count++;
					if (thursday.isSelected())
						count++;
					if (friday.isSelected())
						count++;
					if (saturday.isSelected())
						count++;
					if (sunday.isSelected())
						count++;

					String[] tmp = new String[count];
					count = 0;
					if (monday.isSelected())
						tmp[count++] = "Mandag";
					if (tuesday.isSelected())
						tmp[count++] = "Tirsdag";
					if (wednesday.isSelected())
						tmp[count++] = "Onsdag";
					if (thursday.isSelected())
						tmp[count++] = "Torsdag";
					if (friday.isSelected())
						tmp[count++] = "Fredag";
					if (saturday.isSelected())
						tmp[count++] = "L�rdag";
					if (sunday.isSelected())
						tmp[count++] = "S�ndag";
					s = new StringBuilder();
					for (int i = 0; i < tmp.length - 1; i++) {
						s.append("'" + tmp[i] + "', ");
					}
					s.append("'" + tmp[count - 1] + "'");
					resultSet = statement
							.executeQuery("SELECT ActivityID FROM ACTIVITY_DAY WHERE Day IN ("
									+ s.toString() + ")");

					count = 0;
					resultSet.last();
					dayCount = resultSet.getRow();
					dayID = new int[dayCount];
					resultSet.beforeFirst();
					while (resultSet.next()) {
						dayID[count++] = resultSet.getInt("ActivityID");
					}

				}

				if (city.length() != 0) {
					count = 0;
					resultSet = statement
							.executeQuery("SELECT Zipcode FROM CITY WHERE CITY.City LIKE '%"
									+ city + "%'");
					resultSet.last();
					cityCount = resultSet.getRow();
					resultSet.beforeFirst();
					cityZipcode = new int[cityCount];
					while (resultSet.next()) {
						cityZipcode[count++] = resultSet.getInt("Zipcode");
					}
				}

				String[] condition = new String[12 + dayCount + cityCount];
				int i = 0; // used for condition
				condition[i++] = "SELECT * from ACTIVITY left outer join CITY using (Zipcode) "
						+ "LEFT OUTER JOIN DOCUMENTS using (ActivityID) WHERE ";

				if (dayCount != 0) {
					s = new StringBuilder();
					s.append("ActivityID IN (");
					int j = 0;
					for (; j < dayID.length - 1; j++) {
						s.append("'" + dayID[j] + "',");
					}
					s.append("'" + dayID[j] + "')");
					condition[i++] = s.toString();
				}

				if (cityCount != 0) {
					s = new StringBuilder();
					s.append("Zipcode IN (");
					int j = 0;
					for (; j < cityZipcode.length - 1; j++) {
						s.append("'" + cityZipcode[j] + "',");
					}
					s.append("'" + cityZipcode[j] + "')");
					condition[i++] = s.toString();
				}

				if (name.length() != 0) {
					condition[i++] = "Name LIKE '%" + name + "%'";
				}
				if (manager.length() != 0) {
					condition[i++] = "Manager LIKE '%" + manager + "%'";
				}
				if (adr.length() != 0) {
					condition[i++] = "Address LIKE '%" + adr + "%'";
				}
				if (zipcode.length() != 0) {
					condition[i++] = "Zipcode = '" + zipcode + "'";
				}
				if (seats.length() != 0) {
					condition[i++] = "TotalSeats = '" + seats + "'";
				}
				if (status.equals("Aktiv")) {
					condition[i++] = "EndDate > '"
							+ sqlDateFormat(getCurrentDate()) + "'";
				} else if (status.equals("Inaktiv")) {
					condition[i++] = "EndDate < '"
							+ sqlDateFormat(getCurrentDate()) + "'";
				}
				if (durationStart.length() != 0
						&& !durationStart.equals("dd-mm-����")) {
					condition[i++] = "StartDate = '%"
							+ sqlDateFormat(removeDateLine(durationStart))
							+ "%'";
				}
				if (durationEnd.length() != 0
						&& !durationEnd.equals("dd-mm-����")) {
					condition[i++] = "EndDate = '%"
							+ sqlDateFormat(removeDateLine(durationEnd)) + "%'";
				}
				if (doc.length() != 0)
					condition[i++] = "Document LIKE '%" + doc + "%'";

				s = new StringBuilder();

				if (condition[1] != null) {
					s.append(condition[0]);
					s.append(condition[1]);
					for (i = 2; i < condition.length; i++) {
						if (condition[i] != null) {
							s.append(" OR ");
							s.append(condition[i]);
						}
					}
				} else {
					s.append("SELECT * from ACTIVITY left outer join CITY using (Zipcode) "
							+ "LEFT OUTER JOIN DOCUMENTS using (ActivityID)");
				}
				// query database
				resultSet = statement.executeQuery(s.toString());

				// process query results
				if (resultSet.next()) {
					resultSet.last();
					int row = resultSet.getRow();
					resultSet.beforeFirst();

					JTable table;
					String[] columnName = { " ID ", " Navn ",
							" Ansvarshavende ", " Adresse ", " Postnr ",
							" Poststed ", " StartDato ", " Sluttdato ",
							" Status ", " Dag(er)      ", " Tidspunkt       ",
							" Antall plasser ", " Dokument  " };
					Object[][] values = new Object[row][13];
					for (int refresh = 0; refresh < row; refresh++) {
						values[refresh][12] = ""; // documentfield can be empty
					}

					row = 0;

					ResultSet daySet = null;
					Statement statement2 = connection.createStatement();

					while (resultSet.next()) {
						daySet = statement2
								.executeQuery("SELECT * From ACTIVITY_DAY WHERE ActivityID = '"
										+ resultSet.getString("ActivityID")
										+ "'");
						String[][] tmp = new String[7][2];
						while (daySet.next()) {

							if (daySet.getString("Day").equals("Alle dager")) {
								values[row][9] = " " + daySet.getString("Day");
								values[row][10] = " "
										+ daySet.getString("StartTime")
												.substring(0, 5)
										+ " - "
										+ daySet.getString("EndTime")
												.substring(0, 5);
							} else {
								// not all days

								if (daySet.getString("Day").equals("Mandag")) {
									tmp[0][0] = " " + daySet.getString("Day");
									tmp[0][1] = " "
											+ daySet.getString("StartTime")
													.substring(0, 5)
											+ " - "
											+ daySet.getString("EndTime")
													.substring(0, 5);
								} else if (daySet.getString("Day").equals(
										"Tirsdag")) {
									tmp[1][0] = " " + daySet.getString("Day");
									tmp[1][1] = " "
											+ daySet.getString("StartTime")
													.substring(0, 5)
											+ " - "
											+ daySet.getString("EndTime")
													.substring(0, 5);
								} else if (daySet.getString("Day").equals(
										"Onsdag")) {
									tmp[2][0] = " " + daySet.getString("Day");
									tmp[2][1] = " "
											+ daySet.getString("StartTime")
													.substring(0, 5)
											+ " - "
											+ daySet.getString("EndTime")
													.substring(0, 5);
								} else if (daySet.getString("Day").equals(
										"Torsdag")) {
									tmp[3][0] = " " + daySet.getString("Day");
									tmp[3][1] = " "
											+ daySet.getString("StartTime")
													.substring(0, 5)
											+ " - "
											+ daySet.getString("EndTime")
													.substring(0, 5);
								} else if (daySet.getString("Day").equals(
										"Fredag")) {
									tmp[4][0] = " " + daySet.getString("Day");
									tmp[4][1] = " "
											+ daySet.getString("StartTime")
													.substring(0, 5)
											+ " - "
											+ daySet.getString("EndTime")
													.substring(0, 5);
								} else if (daySet.getString("Day").equals(
										"L�rdag")) {
									tmp[5][0] = " " + daySet.getString("Day");
									tmp[5][1] = " "
											+ daySet.getString("StartTime")
													.substring(0, 5)
											+ " - "
											+ daySet.getString("EndTime")
													.substring(0, 5);
								} else if (daySet.getString("Day").equals(
										"S�ndag")) {
									tmp[6][0] = " " + daySet.getString("Day");
									tmp[6][1] = " "
											+ daySet.getString("StartTime")
													.substring(0, 5)
											+ " - "
											+ daySet.getString("EndTime")
													.substring(0, 5);
								}
							}
						}

						s = new StringBuilder();
						StringBuilder sb = new StringBuilder();
						for (int j = 0; j < tmp.length; j++) {
							if (tmp[j][0] != null) {
								s.append(tmp[j][0] + "\n");
								sb.append(tmp[j][1] + "\n");
							}
						}
						if (s.length() != 0) {
							values[row][9] = s.toString();
							values[row][10] = sb.toString();
						}

						values[row][0] = " "
								+ resultSet.getString("ActivityID");
						values[row][1] = " " + resultSet.getString("Name");
						values[row][2] = " " + resultSet.getString("Manager");
						values[row][3] = " " + resultSet.getString("Address");

						if (resultSet.getString("Zipcode").length() != 4) {
							String zip = "0000"
									+ resultSet.getString("Zipcode");
							values[row][4] = " "
									+ zip.substring(zip.length() - 4,
											zip.length());
						} else
							values[row][4] = " "
									+ resultSet.getString("Zipcode");

						values[row][5] = " " + resultSet.getString("City");
						values[row][6] = " "
								+ outputDateFormat(resultSet
										.getString("StartDate"));
						values[row][7] = " "
								+ outputDateFormat(resultSet
										.getString("EndDate"));

						Calendar calendar = Calendar.getInstance();
						Calendar date = Calendar.getInstance();
						date.set(Integer.parseInt(outputDateFormat(
								resultSet.getString("EndDate")).substring(6)),
								Integer.parseInt(outputDateFormat(
										resultSet.getString("EndDate"))
										.substring(3, 5)) - 1, Integer
										.parseInt(outputDateFormat(
												resultSet.getString("EndDate"))
												.substring(0, 2)));
						calendar.set(calendar.get(Calendar.YEAR),
								calendar.get(Calendar.MONTH),
								calendar.get(Calendar.DAY_OF_MONTH));

						if (calendar.before(date)) {
							values[row][8] = " Aktiv";
						} else {
							values[row][8] = " Inaktiv";
						}

						values[row][11] = " "
								+ resultSet.getString("TotalSeats");

						if (resultSet.getString("Document") != null) {
							int d = resultSet.getString("Document").length() - 1;
							while (resultSet.getString("Document").charAt(d) != '/')
								d--;
							values[row][12] = " "
									+ resultSet.getString("Document")
											.substring(d + 1);
						}

						row++;
					}
					// at least one result
					table = new JTable(
							new DefaultTableModel(values, columnName)) {
						private static final long serialVersionUID = 1L;

						public boolean isCellEditable(int rowIndex, int colIndex) {
							return false; // Disallow the editing of any cell
						}
					};

					table.getColumnModel().getColumn(9)
							.setCellRenderer(new TextAreaRenderer());
					table.getColumnModel().getColumn(10)
							.setCellRenderer(new TextAreaRenderer());

					return table;
				} else {
					// no result
					msg.messageDialog("Fant ingen treff p� ditt s�k", 2);
					return null;
				}

			}// end try
			catch (SQLException sqlException) {
				msg.messageDialog("Kunne ikke fullf�re s�ket", 0);
				initializeDatabase();
				return null;
			}// end catch
		}

		return null;
	}

	public boolean validateSearchActivity(String name, String manager,
			String adr, String zipcode, String city, String seats,
			String durationStart, String durationEnd, String doc) {

		StringBuilder s = new StringBuilder();

		// validate name
		if (name.length() != 0) {
			if (validName(name) == 1) {
				s.append("* Navn p� aktivitet kan bare inneholde\n A-�/a-�, -, ., og mellomrom\n");
			} else if (validName(name) == 2) {
				s.append("* Navn p� aktivitet kan ikke slutte med -\n");
			}
		}

		// validate manager
		if (manager.length() != 0) {
			if (validName(manager) == 1) {
				s.append("* Navn p� ansvarshavende kan bare inneholde\n A-�/a-�, -, ., og mellomrom\n");
			} else if (validName(manager) == 2) {
				s.append("* Navn p� ansvarshavende kan ikke slutte med -\n");
			}
		}

		// validate address
		if (adr.length() != 0 && specialCharacters(adr, "address")) {
			s.append("* Adressen kan ikke inneholde spesialtegn\n");
		}

		// validate zipcode
		if (zipcode.length() != 0) {
			if (zipcode.length() != 4) {
				s.append("* Postnummer m� ha kun 4 siffer\n");
			} else if (!validNr(zipcode)) {
				s.append("* Ugyldig postnummer\n");
			}
		}

		// validate city
		if (city.length() != 0) {
			if (specialCharacters(city, "city")) {
				s.append("* Poststed kan ikke inneholde spesialtegn\n");
			} else {
				for (int i = 0; i < city.length(); i++) {
					if (Character.isDigit(city.charAt(i))) {
						s.append("* Poststed kan ikke inneholde tall\n");
						i = city.length();
					}
				}
			}
		}

		// validate totalSeats
		if (seats.length() != 0 && !validNr(seats)) {
			s.append("* Antall plasser m� v�re et heltall\n");
		}

		// validate duration dates
		SimpleDateFormat dateformat = new SimpleDateFormat("ddMMyyyy");
		dateformat.setLenient(false);
		if (durationStart.length() != 0 && !durationStart.equals("dd-mm-����")) {
			try {
				dateformat.parse(removeDateLine(durationStart));
			} catch (Exception e) {
				s.append("* Startdato m� skrives p� f�lgende format: dd-mm-����\n");
			}
		}
		if (durationEnd.length() != 0 && !durationEnd.equals("dd-mm-����")) {
			try {
				dateformat.parse(removeDateLine(durationEnd));
			} catch (Exception e) {
				s.append("* Sluttdato m� skrives p� f�lgende format: dd-mm-����\n");
			}
		}

		if (s.length() == 0)
			return true;
		else {
			msg.messageDialog(s.toString(), 2);
			return false;
		}
	}

	public boolean doubleLines(String input) {
		for (int i = 0; i < input.length() - 1; i++) {
			if (input.charAt(i) == '-' && input.charAt(i + 1) == '-') {
				return true;
			}
		}
		return false;
	}

	public boolean validComment(String comment) {
		if (specialCharacters(comment, "comment"))
			return false;
		else if (doubleLines(comment))
			return false;
		else
			return true;
	}

	public Activity terminateActivity(String id, String comment) {
		if (validComment(comment)) {
			// query database
			try {
				statement.executeUpdate("BEGIN");
				statement.executeUpdate("UPDATE ACTIVITY SET " + "EndDate = '"
						+ sqlDateFormat(getCurrentDate()) + "'"
						+ "WHERE ActivityID = '" + id + "'");
				addActivityLog(id, "Avsluttet. " + comment);
				statement.executeUpdate("COMMIT");
				msg.messageDialog("Aktiviteten er terminert", 1);
			}// end try
			catch (SQLException sqlException) {
				msg.messageDialog("Kunne ikke terminere aktivitet " + id, 0);
				initializeDatabase();
				return null;
			}// end catch
		} else {
			msg.messageDialog(
					"Kan ikke ha spesialtegn eller -- i kommentarfeltet", 2);
			return null;
		}
		return getActivity(id);
	}

	public Activity editActivity(String id, String name, String manager,
			String adr, String zipcode, String city, String seats,
			String durationStart, String durationEnd, String allTF,
			String allTT, String mondayTF, String mondayTT, String tuesdayTF,
			String tuesdayTT, String wednesdayTF, String wednesdayTT,
			String thursdayTF, String thursdayTT, String fridayTF,
			String fridayTT, String saturdayTF, String saturdayTT,
			String sundayTF, String sundayTT, String description, String path,
			String oldname, String oldmanager, String oldadr,
			String oldzipcode, String oldcity, String oldseats,
			String olddurationStart, String olddurationEnd, String olddesc,
			String oldpath) {

		if (validateEditActivity(name, manager, adr, zipcode, city, seats,
				durationStart, durationEnd, allTF, allTT, mondayTF, mondayTT,
				tuesdayTF, tuesdayTT, wednesdayTF, wednesdayTT, thursdayTF,
				thursdayTT, fridayTF, fridayTT, saturdayTF, saturdayTT,
				sundayTF, sundayTT, description)) {

			Days[] days = getActivity(id).getDays();
			// query database
			try {
				statement.executeUpdate("BEGIN");

				// add zipcode/city if !in table
				if (!(statement
						.executeQuery("SELECT * from CITY where Zipcode = '"
								+ zipcode + "'")).next()) {

					statement.executeUpdate("INSERT INTO CITY VALUES ('"
							+ zipcode + "', '" + capitalizeFully(city) + "')");
				}

				if (description.length() == 0)
					description = "Ingen kommentar tilgjengelig";

				// update activity
				statement.executeUpdate("UPDATE ACTIVITY SET " + "Name = '"
						+ name + "', " + "Address = '" + capitalizeFully(adr)
						+ "', " + "Zipcode = '" + zipcode + "', "
						+ "Manager = '" + capitalizeFully(manager) + "', "
						+ "Description = '" + description + "', "
						+ "StartDate = '" + sqlDateFormat(durationStart)
						+ "', " + "EndDate = '" + sqlDateFormat(durationEnd)
						+ "', " + "TotalSeats = '" + seats + "' "
						+ "WHERE ActivityID = '" + id + "'");

				// add/update/remove days
				if (days[0].getDay().equals("Alle dager")) {
					if (allTF.length() != 0 && allTT.length() != 0) {
						// update
						statement.executeUpdate("UPDATE ACTIVITY_DAY SET "
								+ "StartTime = '" + allTF + "', "
								+ "EndTime = '" + allTT + "' "
								+ "WHERE ActivityID = '" + id + "'");
					} else {
						// remove "alle dager"
						statement
								.executeUpdate("DELETE FROM ACTIVITY_DAY WHERE ActivityID = '"
										+ id + "'");

						// find days to insert
						if (mondayTF.length() != 0 && mondayTT.length() != 0) {
							statement
									.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
											+ "'"
											+ id
											+ "', "
											+ "'Mandag', "
											+ "'"
											+ mondayTF
											+ "', "
											+ "'"
											+ mondayTT + "')");
						}
						if (tuesdayTF.length() != 0 && tuesdayTT.length() != 0) {
							statement
									.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
											+ "'"
											+ id
											+ "', "
											+ "'Tirsdag', "
											+ "'"
											+ tuesdayTF
											+ "', "
											+ "'"
											+ tuesdayTT + "')");
						}
						if (wednesdayTF.length() != 0
								&& wednesdayTT.length() != 0) {
							statement
									.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
											+ "'"
											+ id
											+ "', "
											+ "'Onsdag', "
											+ "'"
											+ wednesdayTF
											+ "', "
											+ "'"
											+ wednesdayTT + "')");
						}
						if (thursdayTF.length() != 0
								&& thursdayTT.length() != 0) {
							statement
									.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
											+ "'"
											+ id
											+ "', "
											+ "'Torsdag', "
											+ "'"
											+ thursdayTF
											+ "', "
											+ "'"
											+ thursdayTT + "')");
						}
						if (fridayTF.length() != 0 && fridayTT.length() != 0) {
							statement
									.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
											+ "'"
											+ id
											+ "', "
											+ "'Fredag', "
											+ "'"
											+ fridayTF
											+ "', "
											+ "'"
											+ fridayTT + "')");
						}
						if (saturdayTF.length() != 0
								&& saturdayTT.length() != 0) {
							statement
									.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
											+ "'"
											+ id
											+ "', "
											+ "'L�rdag', "
											+ "'"
											+ saturdayTF
											+ "', "
											+ "'"
											+ saturdayTT + "')");
						}
						if (sundayTF.length() != 0 && sundayTT.length() != 0) {
							statement
									.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
											+ "'"
											+ id
											+ "', "
											+ "'S�ndag', "
											+ "'"
											+ sundayTF
											+ "', "
											+ "'"
											+ sundayTT + "')");
						}

					}
				} else {
					if (days[0].getDay().equals("Mandag")) {
						if (mondayTF.length() != 0 && mondayTT.length() != 0) {
							// update
							statement.executeUpdate("UPDATE ACTIVITY_DAY SET "
									+ "StartTime = '" + mondayTF + "', "
									+ "EndTime = '" + mondayTT + "' "
									+ "WHERE ActivityID = '" + id + "' "
									+ "AND Day = 'Mandag'");
						} else {
							// remove
							statement
									.executeUpdate("DELETE FROM ACTIVITY_DAY WHERE ActivityID = '"
											+ id + "' " + "AND Day = 'Mandag'");
						}
					} else if (mondayTF.length() != 0 && mondayTT.length() != 0) {
						statement
								.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
										+ "'"
										+ id
										+ "', "
										+ "'Mandag', "
										+ "'"
										+ mondayTF
										+ "', "
										+ "'"
										+ mondayTT
										+ "')");
					}

					if (days[1].getDay().equals("Tirsdag")) {
						if (tuesdayTF.length() != 0 && tuesdayTT.length() != 0) {
							// update
							statement.executeUpdate("UPDATE ACTIVITY_DAY SET "
									+ "StartTime = '" + allTF + "', "
									+ "EndTime = '" + allTT + "' "
									+ "WHERE ActivityID = '" + id + "' "
									+ "AND Day = 'Tirsdag'");
						} else {
							// remove
							statement
									.executeUpdate("DELETE FROM ACTIVITY_DAY WHERE ActivityID = '"
											+ id + "' " + "AND Day = 'Tirsdag'");
						}
					} else if (tuesdayTF.length() != 0
							&& tuesdayTT.length() != 0) {
						statement
								.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
										+ "'"
										+ id
										+ "', "
										+ "'Tirsdag', "
										+ "'"
										+ tuesdayTF
										+ "', "
										+ "'"
										+ tuesdayTT + "')");
					}

					if (days[2].getDay().equals("Onsdag")) {
						if (wednesdayTF.length() != 0
								&& wednesdayTT.length() != 0) {
							// update
							statement.executeUpdate("UPDATE ACTIVITY_DAY SET "
									+ "StartTime = '" + allTF + "', "
									+ "EndTime = '" + allTT + "' "
									+ "WHERE ActivityID = '" + id + "' "
									+ "AND Day = 'Onsdag'");
						} else {
							// remove
							statement
									.executeUpdate("DELETE FROM ACTIVITY_DAY WHERE ActivityID = '"
											+ id + "' " + "AND Day = 'Onsdag'");
						}
					} else if (wednesdayTF.length() != 0
							&& wednesdayTT.length() != 0) {
						statement
								.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
										+ "'"
										+ id
										+ "', "
										+ "'Onsdag', "
										+ "'"
										+ wednesdayTF
										+ "', "
										+ "'"
										+ wednesdayTT + "')");
					}

					if (days[3].getDay().equals("Torsdag")) {
						if (thursdayTF.length() != 0
								&& thursdayTT.length() != 0) {
							// update
							statement.executeUpdate("UPDATE ACTIVITY_DAY SET "
									+ "StartTime = '" + allTF + "', "
									+ "EndTime = '" + allTT + "' "
									+ "WHERE ActivityID = '" + id + "' "
									+ "AND Day = 'Torsdag'");
						} else {
							// remove
							statement
									.executeUpdate("DELETE FROM ACTIVITY_DAY WHERE ActivityID = '"
											+ id + "' " + "AND Day = 'Torsdag'");
						}
					} else if (thursdayTF.length() != 0
							&& thursdayTT.length() != 0) {
						statement
								.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
										+ "'"
										+ id
										+ "', "
										+ "'Torsdag', "
										+ "'"
										+ thursdayTF
										+ "', "
										+ "'"
										+ thursdayTT + "')");
					}

					if (days[4].getDay().equals("Fredag")) {
						if (fridayTF.length() != 0 && fridayTT.length() != 0) {
							// update
							statement.executeUpdate("UPDATE ACTIVITY_DAY SET "
									+ "StartTime = '" + allTF + "', "
									+ "EndTime = '" + allTT + "' "
									+ "WHERE ActivityID = '" + id + "' "
									+ "AND Day = 'Fredag'");
						} else {
							// remove
							statement
									.executeUpdate("DELETE FROM ACTIVITY_DAY WHERE ActivityID = '"
											+ id + "' " + "AND Day = 'Fredag'");
						}
					} else if (fridayTF.length() != 0 && fridayTT.length() != 0) {
						statement
								.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
										+ "'"
										+ id
										+ "', "
										+ "'Fredag', "
										+ "'"
										+ fridayTF
										+ "', "
										+ "'"
										+ fridayTT
										+ "')");
					}

					if (days[5].getDay().equals("L�rdag")) {
						if (saturdayTF.length() != 0
								&& saturdayTT.length() != 0) {
							// update
							statement.executeUpdate("UPDATE ACTIVITY_DAY SET "
									+ "StartTime = '" + allTF + "', "
									+ "EndTime = '" + allTT + "' "
									+ "WHERE ActivityID = '" + id + "' "
									+ "AND Day = 'L�rdag'");
						} else {
							// remove
							statement
									.executeUpdate("DELETE FROM ACTIVITY_DAY WHERE ActivityID = '"
											+ id + "' " + "AND Day = 'L�rdag'");
						}
					} else if (saturdayTF.length() != 0
							&& saturdayTT.length() != 0) {
						statement
								.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
										+ "'"
										+ id
										+ "', "
										+ "'L�rdag', "
										+ "'"
										+ saturdayTF
										+ "', "
										+ "'"
										+ saturdayTT
										+ "')");
					}

					if (days[6].getDay().equals("S�ndag")) {
						if (sundayTF.length() != 0 && sundayTT.length() != 0) {
							// update
							statement.executeUpdate("UPDATE ACTIVITY_DAY SET "
									+ "StartTime = '" + allTF + "', "
									+ "EndTime = '" + allTT + "' "
									+ "WHERE ActivityID = '" + id + "' "
									+ "AND Day = 'S�ndag'");
						} else {
							// remove
							statement
									.executeUpdate("DELETE FROM ACTIVITY_DAY WHERE ActivityID = '"
											+ id + "' " + "AND Day = 'S�ndag'");
						}
					} else if (sundayTF.length() != 0 && sundayTT.length() != 0) {
						statement
								.executeUpdate("INSERT INTO ACTIVITY_DAY VALUES("
										+ "'"
										+ id
										+ "', "
										+ "'S�ndag', "
										+ "'"
										+ sundayTF
										+ "', "
										+ "'"
										+ sundayTT
										+ "')");
					}
				}

				// add/update/delete document-path
				if (!oldpath.equalsIgnoreCase(path)) {

					if (oldpath.length() == 0) {
						statement.executeUpdate("INSERT INTO DOCUMENTS VALUES("
								+ "'" + id + "', " + "'" + path + "')");
					} else if (path.length() == 0) {
						statement
								.executeUpdate("DELETE FROM DOCUMENTS WHERE ActivityID = '"
										+ id + "'");
					} else {
						statement
								.executeUpdate("UPDATE DOCUMENTS SET Document = '"
										+ path
										+ "' WHERE ActivityID = '"
										+ id
										+ "'");
					}
				}

				// add changes to activityLog
				if (!oldname.equalsIgnoreCase(name)) {
					addActivityLog(id, "Aktivitetsnavn endret fra \"" + oldname
							+ "\" til \"" + capitalizeFully(name) + "\"");
				}

				if (!oldmanager.equalsIgnoreCase(manager)) {
					addActivityLog(id, "Avsvarshavende endret fra \""
							+ oldmanager + "\" til \""
							+ capitalizeFully(manager) + "\"");
				}

				if (!oldadr.equalsIgnoreCase(adr)) {
					addActivityLog(id, "Adresse endret fra \"" + oldadr
							+ "\" til \"" + capitalizeFully(adr) + "\"");
				}

				if (!oldzipcode.equals(zipcode)
						| !oldcity.equalsIgnoreCase(city)) {
					addMemberLog(id, "Poststed endret fra \"" + oldzipcode
							+ " " + capitalizeFully(oldcity) + "\" til \""
							+ zipcode + " " + capitalizeFully(city) + "\"");
				}

				if (!oldseats.equalsIgnoreCase(seats)) {
					addActivityLog(id, "Antall plasser endret fra \""
							+ oldseats + "\" til \"" + seats + "\"");
				}

				if (!removeDateLine(olddurationStart).equalsIgnoreCase(
						removeDateLine(durationStart))
						| !removeDateLine(olddurationEnd).equalsIgnoreCase(
								removeDateLine(durationEnd))) {
					addActivityLog(id, "Varighet endret fra \""
							+ olddurationStart + " - " + olddurationEnd
							+ "\" til \"" + durationStart + " - " + durationEnd
							+ "\"");
				}

				if (days[0].getDay().equals("Alle dager")) {
					if (!days[0].getStartTime().equals(allTF)
							| !days[0].getEndTime().equals(allTT)) {
						addActivityLog(id, "Tidspunktet er endret");
					}
				} else {
					if (days[0].getDay().equals("Mandag")) {
						if (!days[0].getStartTime().equals(mondayTF)
								| !days[0].getEndTime().equals(mondayTT)) {
							addActivityLog(id, "Tidspunktet er endret");
						}
					} else if (days[1].getDay().equals("Tirsdag")) {
						if (!days[1].getStartTime().equals(tuesdayTF)
								| !days[1].getEndTime().equals(tuesdayTT)) {
							addActivityLog(id, "Tidspunktet er endret");
						}
					} else if (days[2].getDay().equals("Onsdag")) {
						if (!days[2].getStartTime().equals(wednesdayTF)
								| !days[2].getEndTime().equals(wednesdayTT)) {
							addActivityLog(id, "Tidspunktet er endret");
						}
					} else if (days[3].getDay().equals("Torsdag")) {
						if (!days[3].getStartTime().equals(thursdayTF)
								| !days[3].getEndTime().equals(thursdayTT)) {
							addActivityLog(id, "Tidspunktet er endret");
						}
					} else if (days[4].getDay().equals("Fredag")) {
						if (!days[4].getStartTime().equals(fridayTF)
								| !days[4].getEndTime().equals(fridayTT)) {
							addActivityLog(id, "Tidspunktet er endret");
						}
					} else if (days[5].getDay().equals("L�rdag")) {
						if (!days[5].getStartTime().equals(saturdayTF)
								| !days[5].getEndTime().equals(saturdayTT)) {
							addActivityLog(id, "Tidspunktet er endret");
						}
					} else if (days[6].getDay().equals("S�ndag")) {
						if (!days[6].getStartTime().equals(sundayTF)
								| !days[6].getEndTime().equals(sundayTT)) {
							addActivityLog(id, "Tidspunktet er endret");
						}
					}
				}

				if (!olddesc.equalsIgnoreCase(description)) {
					addActivityLog(id, "Beskrivelsen er endret");
				}

				if (!oldpath.equalsIgnoreCase(path)) {

					if (oldpath.length() == 0) {
						String[] doc = path.split("/");
						addActivityLog(id, "Dokumentet " + doc[doc.length - 1]
								+ " er lagt til");
					} else if (path.length() == 0) {
						String[] doc = oldpath.split("/");
						addActivityLog(id, "Dokumentet " + doc[doc.length - 1]
								+ " er fjernet");
					} else {
						addActivityLog(id, "Opplastet dokument er endret");
					}
				}

				msg.messageDialog("Aktiviteten er oppdatert", 1);
				statement.executeUpdate("COMMIT");
				return getActivity(id);
			} // end try
			catch (SQLException sqlException) {
				msg.messageDialog("Kunne ikke oppdatere aktiviteten", 0);
				initializeDatabase();
				return null;
			}// end catch
		}
		return null;
	}

	@SuppressWarnings("deprecation")
	public boolean validateEditActivity(String name, String manager,
			String adr, String zipcode, String city, String seats,
			String durationStart, String durationEnd, String allTF,
			String allTT, String mondayTF, String mondayTT, String tuesdayTF,
			String tuesdayTT, String wednesdayTF, String wednesdayTT,
			String thursdayTF, String thursdayTT, String fridayTF,
			String fridayTT, String saturdayTF, String saturdayTT,
			String sundayTF, String sundayTT, String description) {

		StringBuilder s = new StringBuilder();
		// validate name
		if (name.length() == 0) {
			s.append("* Navn p� aktivitet p� fylles ut\n");
		} else if (validName(name) == 1) {
			s.append("* Navn p� aktivitet kan bare inneholde\n A-�/a-�, -, ., og mellomrom\n");
		} else if (validName(name) == 2) {
			s.append("* Navn p� aktivitet kan ikke slutte med -\n");
		}

		// validate manager
		if (manager.length() == 0) {
			s.append("* Navn p� ansvarshavende m� fylles ut\n");
		} else if (validName(manager) == 1) {
			s.append("* Navn p� ansvarshavende kan bare inneholde\n A-�/a-�, -, ., og mellomrom\n");
		} else if (validName(manager) == 2) {
			s.append("* Navn p� ansvarshavende kan ikke slutte med -\n");
		}

		// validate address
		if (adr.length() == 0) {
			s.append("* Adressen m� fylles ut\n");
		} else if (specialCharacters(adr, "address")) {
			s.append("* Adressen kan ikke inneholde spesialtegn\n");
		}

		// validate zipcode
		if (zipcode.length() == 0) {
			s.append("* Postnummer m� fylles ut\n");
		} else if (zipcode.length() != 4) {
			s.append("* Postnummer m� ha kun 4 siffer\n");
		} else if (!validNr(zipcode)) {
			s.append("* Ugyldig postnummer\n");
		}

		// validate city
		if (city.length() == 0) {
			s.append("* Poststed m� fylles ut\n");
		} else if (specialCharacters(city, "city")) {
			s.append("* Poststed kan ikke inneholde spesialtegn\n");
		} else {
			for (int i = 0; i < city.length(); i++) {
				if (Character.isDigit(city.charAt(i))) {
					s.append("* Poststed kan ikke inneholde tall\n");
					i = city.length();
				}
			}
		}

		// validate totalSeats
		if (seats.length() == 0) {
			s.append("* Antall plasser p� fylles ut\n");
		} else if (!validNr(seats)) {
			s.append("* Antall plasser m� v�re et heltall\n");
		}

		// validate duration dates
		if ((durationStart.length() == 0 | durationStart.equals("dd-mm-����"))
				| (durationEnd.length() == 0 | durationEnd.equals("dd-mm-����"))) {
			s.append("* Begge datofeltene m� fylles ut\n");
		} else {
			boolean okDate = true;
			// validate startdate
			durationStart = removeDateLine(durationStart);
			try {
				SimpleDateFormat dateformat = new SimpleDateFormat("ddMMyyyy");
				dateformat.setLenient(false);
				dateformat.parse(durationStart);

			} catch (Exception e) {
				okDate = false;
				s.append("* Startdato m� skrives p� f�lgende format: dd-mm-����\n");
			}

			// validate enddate
			if (okDate) {
				durationEnd = removeDateLine(durationEnd);
				try {
					SimpleDateFormat dateformat = new SimpleDateFormat(
							"ddMMyyyy");
					dateformat.setLenient(false);
					dateformat.parse(durationEnd);

					if (dateformat.parse(durationEnd).before(
							dateformat.parse(durationStart))) {
						s.append("* Sluttdato kan ikke v�re f�r startdato\n");
					}
				} catch (Exception e) {
					s.append("* Sluttdato m� skrives p� f�lgende format: dd-mm-����\n");
				}

			}

			if (okDate) {
				if (Integer.parseInt(durationStart.substring(4)) < 1900 + Calendar
						.getInstance().getTime().getYear() - 1
						| Integer.parseInt(durationStart.substring(4)) > 1900 + Calendar
								.getInstance().getTime().getYear() + 1) {
					s.append("* Startdato m� v�re innenfor �rene "
							+ (1900 + Calendar.getInstance().getTime()
									.getYear() - 1)
							+ " - "
							+ (1900 + Calendar.getInstance().getTime()
									.getYear() + 1) + "\n");
				}
			}

		}

		// validate time
		// non of the fields have input
		if (allTF.length() == 0 && allTT.length() == 0
				&& mondayTF.length() == 0 && mondayTT.length() == 0
				&& tuesdayTF.length() == 0 && tuesdayTT.length() == 0
				&& wednesdayTF.length() == 0 && wednesdayTT.length() == 0
				&& thursdayTF.length() == 0 && thursdayTT.length() == 0
				&& fridayTF.length() == 0 && fridayTT.length() == 0
				&& saturdayTF.length() == 0 && saturdayTT.length() == 0
				&& sundayTF.length() == 0 && sundayTT.length() == 0) {
			s.append("* Tidspunktet m� fylles ut");
		} else {
			if ((allTF.length() != 0 && allTT.length() == 0)
					| (allTF.length() == 0 && allTT.length() != 0)) {
				s.append("* Begge feltene i tidspunktet for \"alle dager\" m� fylles ut\n");
			} else if (allTF.length() != 0 && allTT.length() != 0) {
				if (!validTime(allTF) | !validTime(allTT)) {
					s.append("* Tidspunktet for \"alle dager\" er ugyldig\nKorrekt format er tt:mm\n");
				} else {

					try {
						if (new SimpleDateFormat("HH:mm").parse(allTF).after(
								new SimpleDateFormat("HH:mm").parse(allTT)))
							s.append("* Starttiden for \"alle dager\" m� v�re f�r sluttiden\n");
					} catch (ParseException e) {
						// Invalid date was entered
					}
				}
			} else {
				// all doesn't have input

				// check monday
				if ((mondayTF.length() != 0 && mondayTT.length() == 0)
						| (mondayTF.length() == 0 && mondayTT.length() != 0)) {
					s.append("* Begge feltene i tidspunktet for mandag m� fylles ut\n");
				} else if (mondayTF.length() != 0 && mondayTT.length() != 0) {
					if (!validTime(mondayTF) | !validTime(mondayTT)) {
						s.append("* Tidspunktet for mandag er ugyldig\nKorrekt format er tt:mm\n");
					} else {

						try {
							if (new SimpleDateFormat("HH:mm").parse(mondayTF)
									.after(new SimpleDateFormat("HH:mm")
											.parse(mondayTT)))
								s.append("* Starttiden for mandag m� v�re f�r sluttiden\n");
						} catch (ParseException e) {
							// Invalid date was entered
						}
					}
				}

				// check tuesday
				if ((tuesdayTF.length() != 0 && tuesdayTT.length() == 0)
						| (tuesdayTF.length() == 0 && tuesdayTT.length() != 0)) {
					s.append("* Begge feltene i tidspunktet for tirsdag m� fylles ut\n");
				} else if (tuesdayTF.length() != 0 && tuesdayTT.length() != 0) {
					if (!validTime(tuesdayTF) | !validTime(tuesdayTT)) {
						s.append("* Tidspunktet for tirsdag er ugyldig\nKorrekt format er tt:mm\n");
					} else {

						try {
							if (new SimpleDateFormat("HH:mm").parse(tuesdayTF)
									.after(new SimpleDateFormat("HH:mm")
											.parse(tuesdayTT)))
								s.append("* Starttiden for tirsdag m� v�re f�r sluttiden\n");
						} catch (ParseException e) {
							// Invalid date was entered
						}
					}
				}

				// check wednesday
				if ((wednesdayTF.length() != 0 && wednesdayTT.length() == 0)
						| (wednesdayTF.length() == 0 && wednesdayTT.length() != 0)) {
					s.append("* Begge feltene i tidspunktet for onsdag m� fylles ut\n");
				} else if (wednesdayTF.length() != 0
						&& wednesdayTT.length() != 0) {
					if (!validTime(wednesdayTF) | !validTime(wednesdayTT)) {
						s.append("* Tidspunktet for onsdag er ugyldig\nKorrekt format er tt:mm\n");
					} else {

						try {
							if (new SimpleDateFormat("HH:mm")
									.parse(wednesdayTF).after(
											new SimpleDateFormat("HH:mm")
													.parse(wednesdayTT)))
								s.append("* Starttiden for onsdag m� v�re f�r sluttiden\n");
						} catch (ParseException e) {
							// Invalid date was entered
						}
					}
				}

				// check thursday
				if ((thursdayTF.length() != 0 && thursdayTT.length() == 0)
						| (thursdayTF.length() == 0 && thursdayTT.length() != 0)) {
					s.append("* Begge feltene i tidspunktet for torsdag m� fylles ut\n");
				} else if (thursdayTF.length() != 0 && thursdayTT.length() != 0) {
					if (!validTime(thursdayTF) | !validTime(thursdayTT)) {
						s.append("* Tidspunktet for torsdag er ugyldig\nKorrekt format er tt:mm\n");
					} else {

						try {
							if (new SimpleDateFormat("HH:mm").parse(thursdayTF)
									.after(new SimpleDateFormat("HH:mm")
											.parse(thursdayTT)))
								s.append("* Starttiden for torsdag m� v�re f�r sluttiden\n");
						} catch (ParseException e) {
							// Invalid date was entered
						}
					}
				}

				// check friday
				if ((fridayTF.length() != 0 && fridayTT.length() == 0)
						| (fridayTF.length() == 0 && fridayTT.length() != 0)) {
					s.append("* Begge feltene i tidspunktet for fredag m� fylles ut\n");
				} else if (fridayTF.length() != 0 && fridayTT.length() != 0) {
					if (!validTime(fridayTF) | !validTime(fridayTT)) {
						s.append("* Tidspunktet for fredag er ugyldig\nKorrekt format er tt:mm\n");
					} else {

						try {
							if (new SimpleDateFormat("HH:mm").parse(fridayTF)
									.after(new SimpleDateFormat("HH:mm")
											.parse(fridayTT)))
								s.append("* Starttiden for fredag m� v�re f�r sluttiden\n");
						} catch (ParseException e) {
							// Invalid date was entered
						}
					}
				}

				// check saturday
				if ((saturdayTF.length() != 0 && saturdayTT.length() == 0)
						| (saturdayTF.length() == 0 && saturdayTT.length() != 0)) {
					s.append("* Begge feltene i tidspunktet for l�rdag m� fylles ut\n");
				} else if (saturdayTF.length() != 0 && saturdayTT.length() != 0) {
					if (!validTime(saturdayTF) | !validTime(saturdayTT)) {
						s.append("* Tidspunktet for l�rdag er ugyldig\nKorrekt format er tt:mm\n");
					} else {

						try {
							if (new SimpleDateFormat("HH:mm").parse(saturdayTF)
									.after(new SimpleDateFormat("HH:mm")
											.parse(saturdayTT)))
								s.append("* Starttiden for l�rdag m� v�re f�r sluttiden\n");
						} catch (ParseException e) {
							// Invalid date was entered
						}
					}
				}

				// check sunday
				if ((sundayTF.length() != 0 && sundayTT.length() == 0)
						| (sundayTF.length() == 0 && sundayTT.length() != 0)) {
					s.append("* Begge feltene i tidspunktet for s�ndag m� fylles ut\n");
				} else if (sundayTF.length() != 0 && sundayTT.length() != 0) {
					if (!validTime(sundayTF) | !validTime(sundayTT)) {
						s.append("* Tidspunktet for s�ndag er ugyldig\nKorrekt format er tt:mm\n");
					} else {

						try {
							if (new SimpleDateFormat("HH:mm").parse(sundayTF)
									.after(new SimpleDateFormat("HH:mm")
											.parse(sundayTT)))
								s.append("* Starttiden for s�ndag m� v�re f�r sluttiden\n");
						} catch (ParseException e) {
							// Invalid date was entered
						}
					}
				}
			}
		}

		if (s.length() == 0)
			return true;
		else {
			msg.messageDialog(s.toString(), 2);
			return false;
		}
	}

	public boolean addActivityAttendance(String id, String duration,
			String date, String seats, String comment) {
		if (validateAddActivityAttendance(duration, date, seats, comment)) {
			// query database
			try {
				statement.executeUpdate("BEGIN");
				statement
						.executeUpdate("INSERT INTO ATTENDANCE VALUES ("
								+ "'"
								+ id
								+ "', "
								+ "'"
								+ sqlDateFormat(removeDateLine(date))
								+ "', "
								+ "'"
								+ seats
								+ "', "
								+ "'"
								+ (comment.length() == 0 ? "Ingen kommentar tilgjengelig"
										: comment) + "')");
				addActivityLog(id, "Registrert oppm�te for "
						+ outputDateFormat(sqlDateFormat(removeDateLine(date))));
				statement.executeUpdate("COMMIT");
				msg.messageDialog("Oppm�telisten er oppdatert", 1);
				return true;
			} // end try
			catch (SQLException sqlException) {
				msg.messageDialog("Kunne ikke registrere oppm�tet", 0);
				initializeDatabase();
				return false;
			}// end catch
		}
		return false;
	}

	public boolean validateAddActivityAttendance(String duration, String date,
			String seats, String comment) {

		String[] d = duration.split(" - ");
		StringBuilder s = new StringBuilder();

		// validate date
		date = removeDateLine(date);
		try {
			SimpleDateFormat dateformat = new SimpleDateFormat("ddMMyyyy");
			dateformat.setLenient(false);
			dateformat.parse(date);
			if (dateformat.parse(date).before(
					dateformat.parse(removeDateLine(d[0].toString().trim())))
					| dateformat.parse(date).after(
							dateformat.parse(removeDateLine(d[1].substring(0,
									10).trim())))) {
				s.append("* Datoen er utenfor aktivitetsperioden " + duration
						+ "\n");
			}

		} catch (Exception e) {
			s.append("* Dato m� skrives p� f�lgende format: dd-mm-����\n");
		}

		// validate totalSeats
		if (seats.length() == 0) {
			s.append("* Antall plasser m� fylles ut\n");
		} else if (!validNr(seats)) {
			s.append("* Antall plasser m� v�re et heltall\n");
		}

		// validate comment
		if (!validComment(comment)) {
			s.append("* Kommentar kan ikke inneholde spesialtegn eller --\n");
		}

		if (s.length() == 0)
			return true;
		else {
			msg.messageDialog(s.toString(), 2);
			return false;
		}
	}

	public String[] getMainStatistic() {
		String[] s = { "Utilgjengelig", "Utilgjengelig", "Utilgjengelig" };
		String aCount = "0 (Aktvitet)";
		String mCount = "0 (Medlem)";
		String aLastWeek = "--- (Aktvitet)";
		String mLastWeek = "--- (Medlem)";
		String aLastMonth = "--- (Aktvitet)";
		String mLastMonth = "--- (Medlem)";
		try {

			// get new activity log-entries
			resultSet = statement
					.executeQuery("SELECT COUNT(*) FROM ACTIVITYLOG WHERE DATE(DateTime) = '"
							+ sqlDateFormat(getCurrentDate()) + "'");
			if (resultSet.next()) {
				aCount = resultSet.getString("COUNT(*)") + " (Aktvitet)";
			}
			// get new member log-entries
			resultSet = statement
					.executeQuery("SELECT COUNT(*) FROM MEMBERLOG WHERE DATE(DateTime) = '"
							+ sqlDateFormat(getCurrentDate()) + "'");
			if (resultSet.next()) {
				mCount = resultSet.getString("COUNT(*)") + " (Medlem)";
			}

			s[0] = aCount + " / " + mCount;

			// get previous week's most active user
			Calendar now = Calendar.getInstance();

			if (now.get(Calendar.WEEK_OF_YEAR) == 1) {
				resultSet = statement
						.executeQuery("SELECT MAX(USER) FROM ACTIVITYLOG WHERE WEEK(ACTIVITYLOG.DateTime) = '52' AND YEAR(DateTime) = '"
								+ (Integer.parseInt(getCurrentDate().substring(
										6)) - 1) + "' GROUP BY USER");

				if (resultSet.next()) {
					aLastWeek = resultSet.getString("MAX(USER)")
							+ " (Aktvitet)";
				}

				resultSet = statement
						.executeQuery("SELECT MAX(USER) FROM MEMBERLOG WHERE WEEK(DateTime) = '52' AND YEAR(DateTime) = '"
								+ (Integer.parseInt(getCurrentDate().substring(
										6)) - 1) + "' GROUP BY USER");

				if (resultSet.next()) {
					mLastWeek = resultSet.getString("MAX(USER)") + " (Medlem)";
				}

			} else {
				resultSet = statement
						.executeQuery("SELECT MAX(USER) FROM ACTIVITYLOG WHERE WEEK(DateTime) = '"
								+ (now.get(Calendar.WEEK_OF_YEAR) - 1)
								+ "' GROUP BY USER");

				if (resultSet.next()) {
					aLastWeek = resultSet.getString("MAX(USER)")
							+ " (Aktvitet)";
				}

				resultSet = statement
						.executeQuery("SELECT MAX(USER) FROM MEMBERLOG WHERE WEEK(DateTime) = '"
								+ (now.get(Calendar.WEEK_OF_YEAR) - 1)
								+ "' GROUP BY USER");

				if (resultSet.next()) {
					mLastWeek = resultSet.getString("MAX(USER)") + " (Medlem)";
				}
			}

			s[1] = aLastWeek + " / " + mLastWeek;

			// get previous month's most active user
			if (Integer.parseInt(getCurrentDate().substring(3, 5)) == 1) {
				resultSet = statement
						.executeQuery("SELECT MAX(USER) FROM ACTIVITYLOG WHERE MONTH(DateTime) = '12' AND YEAR(DateTime) = '"
								+ (Integer.parseInt(getCurrentDate().substring(
										6)) - 1) + "' GROUP BY USER");

				if (resultSet.next()) {
					aLastMonth = resultSet.getString("MAX(USER)")
							+ " (Aktvitet)";
				}

				resultSet = statement
						.executeQuery("SELECT MAX(USER) FROM MEMBERLOG WHERE MONTH(DateTime) = '12' AND YEAR(DateTime) = '"
								+ (Integer.parseInt(getCurrentDate().substring(
										6)) - 1) + "' GROUP BY USER");

				if (resultSet.next()) {
					mLastMonth = resultSet.getString("MAX(USER)") + " (Medlem)";
				}

			} else {
				resultSet = statement
						.executeQuery("SELECT MAX(USER) FROM ACTIVITYLOG WHERE MONTH(DateTime) = '"
								+ Integer.parseInt(getCurrentDate().substring(
										3, 5)) + "' GROUP BY USER");

				if (resultSet.next()) {
					aLastMonth = resultSet.getString("MAX(USER)")
							+ " (Aktvitet)";
				}

				resultSet = statement
						.executeQuery("SELECT MAX(USER) FROM MEMBERLOG WHERE MONTH(DateTime) = '"
								+ Integer.parseInt(getCurrentDate().substring(
										3, 5)) + "' GROUP BY USER");

				if (resultSet.next()) {
					mLastMonth = resultSet.getString("MAX(USER)") + " (Medlem)";
				}
			}
			s[2] = aLastMonth + " / " + mLastMonth;
		} // end try
		catch (SQLException sqlException) {
			msg.messageDialog("Kunne ikke hente statistikk", 0);
			initializeDatabase();
		}// end catch
		return s;
	}

	public JTable[] getMemberStatistic(String year, FontMetrics fontmetrics) {
		if (year.length() == 0) {
			msg.messageDialog("* �rstall m� fylles ut", 2);
			return null;
		} else if (!validNr(year)) {
			msg.messageDialog("* �rstall m� inneholde 4 siffer", 2);
			return null;
		} else if (Integer.parseInt(year) > Integer.parseInt(getCurrentDate()
				.substring(6))) {
			msg.messageDialog("* Kan bare hente statistikk frem til "
					+ getCurrentDate().substring(6), 2);
			return null;
		} else {
			JTable[] statTable = new JTable[6];
			// First table data (number of membership) TYPE
			String[] c1 = { " ", " Opprettet", " Avsluttet", " Totalt " };
			String[][] v1 = new String[3][4];
			v1[0][0] = " Vanlig medlem";
			v1[1][0] = " St�ttemedlem";
			v1[2][0] = "<html><b>&nbsp;Totalt</b></html>";

			// Second table data (number of membership) GENDER
			String[] c2 = { " ", " Opprettet", " Avsluttet", " Totalt " };
			String[][] v2 = new String[3][4];
			v2[0][0] = " Mann";
			v2[1][0] = " Kvinne";
			v2[2][0] = "<html><b>&nbsp;Totalt</b></html>";

			// Third table data (age) TYPE
			String[] c3 = { " ", " >18 ", " 18 - 25 ", " 26 - 40 ",
					" 41 - 50 ", " 50< " };
			String[][] v3 = new String[3][6];
			v3[0][0] = " Vanlig medlem";
			v3[1][0] = " St�ttemedlem";
			v3[2][0] = "<html><b>&nbsp;Totalt</b></html>";

			// Fourth table data (age) GENDER
			String[] c4 = { " ", " >18 ", " 18 - 25 ", " 26 - 40 ",
					" 41 - 50 ", " 50< " };
			String[][] v4 = new String[3][6];
			v4[0][0] = " Mann";
			v4[1][0] = " Kvinne";
			v4[2][0] = "<html><b>&nbsp;Totalt</b></html>";

			// Fifth table data (youngest/oldest member) TYPE
			String[] c5 = { " ", " Yngste ", " Eldste ", " Snitt " };
			String[][] v5 = new String[2][4];
			v5[0][0] = " Vanlig medlem";
			v5[1][0] = " St�ttemedlem";

			// Sixth table data (youngest/oldest member) GENDER
			String[] c6 = { " ", " Yngste ", " Eldste ", " Snitt " };
			String[][] v6 = new String[2][4];
			v6[0][0] = " Mann";
			v6[1][0] = " Kvinne";

			try {
				// get total number of member in that year
				resultSet = statement
						.executeQuery("SELECT COUNT(*) AS Total, R.Nr AS Regular, (count(*)-R.Nr) AS Support, G.W AS Women, (count(*)-G.W) AS Men FROM MEMBER, "
								+ "(SELECT COUNT(*) AS Nr FROM MEMBER WHERE "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "' AND "
								+ "MembershipType = 'Vanlig medlem') AS R, "
								+ "(SELECT COUNT(*) AS W FROM MEMBER WHERE "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "' AND "
								+ "Gender = 'Kvinne') AS G "
								+ "WHERE YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1) + "'");

				if (resultSet.next()) {
					v1[0][3] = " " + resultSet.getString("Regular");
					v1[1][3] = " " + resultSet.getString("Support");
					v1[2][3] = "<html><b>&nbsp;" + resultSet.getString("Total")
							+ "</b></html>";

					v2[0][3] = " " + resultSet.getString("Men");
					v2[1][3] = " " + resultSet.getString("Women");
					v2[2][3] = "<html><b>&nbsp;" + resultSet.getString("Total")
							+ "</b></html>";
				}

				// get new member from that year
				resultSet = statement
						.executeQuery("SELECT COUNT(*) AS Total, R.Nr AS Regular, (count(*)-R.Nr) AS Support, G.W AS Women, (count(*)-G.W) AS Men FROM MEMBER, "
								+ "(SELECT COUNT(*) AS Nr FROM MEMBER WHERE "
								+ "YEAR(EnrollmentDate) = '"
								+ year
								+ "' AND "
								+ "MembershipType = 'Vanlig medlem') AS R, "
								+ "(SELECT COUNT(*) AS W FROM MEMBER WHERE "
								+ "YEAR(EnrollmentDate) = '"
								+ year
								+ "' AND "
								+ "Gender = 'Kvinne') AS G "
								+ "WHERE YEAR(EnrollmentDate) = '" + year + "'");

				if (resultSet.next()) {
					v1[0][1] = " " + resultSet.getString("Regular");
					v1[1][1] = " " + resultSet.getString("Support");
					v1[2][1] = "<html><b>&nbsp;" + resultSet.getString("Total")
							+ "</b></html>";

					v2[0][1] = " " + resultSet.getString("Men");
					v2[1][1] = " " + resultSet.getString("Women");
					v2[2][1] = "<html><b>&nbsp;" + resultSet.getString("Total")
							+ "</b></html>";
				}

				// get the number of membership-terminations from that year
				resultSet = statement
						.executeQuery("SELECT COUNT(*) AS Total, R.Nr AS Regular, (count(*)-R.Nr) AS Support, G.W AS Women, (count(*)-G.W) AS Men FROM MEMBER, "
								+ "(SELECT COUNT(*) AS Nr FROM MEMBER WHERE "
								+ "YEAR(TerminationDate) = '"
								+ year
								+ "' AND "
								+ "MembershipType = 'Vanlig medlem') AS R, "
								+ "(SELECT COUNT(*) AS W FROM MEMBER WHERE "
								+ "YEAR(TerminationDate) = '"
								+ year
								+ "' AND "
								+ "Gender = 'Kvinne') AS G "
								+ "WHERE YEAR(TerminationDate) = '"
								+ year
								+ "'");

				if (resultSet.next()) {
					v1[0][2] = " " + resultSet.getString("Regular");
					v1[1][2] = " " + resultSet.getString("Support");
					v1[2][2] = "<html><b>&nbsp;" + resultSet.getString("Total")
							+ "</b></html>";

					v2[0][2] = " " + resultSet.getString("Men");
					v2[1][2] = " " + resultSet.getString("Women");
					v2[2][2] = "<html><b>&nbsp;" + resultSet.getString("Total")
							+ "</b></html>";

				}

				// get age difference from that year

				int[] difference = new int[5];
				// get total difference
				resultSet = statement
						.executeQuery("SELECT under.eighteen, eighteen.twentyfive, twentysix.forty, fortyone.fifty, above.fifty FROM "
								+ "(SELECT COUNT(*) AS eighteen FROM MEMBER WHERE YEAR(DateOfBirth) > '"
								+ (Integer.parseInt(year) - 18)
								+ "' AND "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "') AS under, "
								+ "(SELECT COUNT(*) AS twentyfive FROM MEMBER WHERE YEAR(DateOfBirth) >= '"
								+ (Integer.parseInt(year) - 25)
								+ "' AND "
								+ "YEAR(DateOfBirth) < '"
								+ (Integer.parseInt(year) - 19)
								+ "' AND "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "') AS eighteen, "
								+ "(SELECT COUNT(*) AS forty FROM MEMBER WHERE YEAR(DateOfBirth) >= '"
								+ (Integer.parseInt(year) - 40)
								+ "' AND "
								+ "YEAR(DateOfBirth) < '"
								+ (Integer.parseInt(year) - 27)
								+ "' AND "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "') AS twentysix, "
								+ "(SELECT COUNT(*) AS fifty FROM MEMBER WHERE YEAR(DateOfBirth) >= '"
								+ (Integer.parseInt(year) - 50)
								+ "' AND "
								+ "YEAR(DateOfBirth) < '"
								+ (Integer.parseInt(year) - 42)
								+ "' AND "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "') AS fortyone, "
								+ "(SELECT COUNT(*) AS fifty FROM MEMBER WHERE YEAR(DateOfBirth) < '"
								+ (Integer.parseInt(year) - 50)
								+ "' AND "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1) + "') AS above");

				if (resultSet.next()) {
					v3[2][1] = "<html><b>&nbsp;" + resultSet.getString(1)
							+ "</b></html>";
					v3[2][2] = "<html><b>&nbsp;" + resultSet.getString(2)
							+ "</b></html>";
					v3[2][3] = "<html><b>&nbsp;" + resultSet.getString(3)
							+ "</b></html>";
					v3[2][4] = "<html><b>&nbsp;" + resultSet.getString(4)
							+ "</b></html>";
					v3[2][5] = "<html><b>&nbsp;" + resultSet.getString(5)
							+ "</b></html>";

					v4[2][1] = "<html><b>&nbsp;" + resultSet.getString(1)
							+ "</b></html>";
					v4[2][2] = "<html><b>&nbsp;" + resultSet.getString(2)
							+ "</b></html>";
					v4[2][3] = "<html><b>&nbsp;" + resultSet.getString(3)
							+ "</b></html>";
					v4[2][4] = "<html><b>&nbsp;" + resultSet.getString(4)
							+ "</b></html>";
					v4[2][5] = "<html><b>&nbsp;" + resultSet.getString(5)
							+ "</b></html>";

					difference[0] = resultSet.getInt(1);
					difference[1] = resultSet.getInt(2);
					difference[2] = resultSet.getInt(3);
					difference[3] = resultSet.getInt(4);
					difference[4] = resultSet.getInt(5);
				}

				// get age difference based on membership-type
				resultSet = statement
						.executeQuery("SELECT under.eighteen, eighteen.twentyfive, twentysix.forty, fortyone.fifty, above.fifty FROM "
								+ "(SELECT COUNT(*) AS eighteen FROM MEMBER WHERE YEAR(DateOfBirth) > '"
								+ (Integer.parseInt(year) - 18)
								+ "'"
								+ " AND MembershipType = 'Vanlig medlem' AND "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "') AS under, "
								+ "(SELECT COUNT(*) AS twentyfive FROM MEMBER WHERE YEAR(DateOfBirth) >= '"
								+ (Integer.parseInt(year) - 25)
								+ "' AND "
								+ "YEAR(DateOfBirth) < '"
								+ (Integer.parseInt(year) - 19)
								+ "'"
								+ " AND MembershipType = 'Vanlig medlem' AND "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "') AS eighteen, "
								+ "(SELECT COUNT(*) AS forty FROM MEMBER WHERE YEAR(DateOfBirth) >= '"
								+ (Integer.parseInt(year) - 40)
								+ "' AND "
								+ "YEAR(DateOfBirth) < '"
								+ (Integer.parseInt(year) - 27)
								+ "'"
								+ " AND MembershipType = 'Vanlig medlem' AND "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "') AS twentysix, "
								+ "(SELECT COUNT(*) AS fifty FROM MEMBER WHERE YEAR(DateOfBirth) >= '"
								+ (Integer.parseInt(year) - 50)
								+ "' AND "
								+ "YEAR(DateOfBirth) < '"
								+ (Integer.parseInt(year) - 42)
								+ "'"
								+ " AND MembershipType = 'Vanlig medlem' AND "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "') AS fortyone, "
								+ "(SELECT COUNT(*) AS fifty FROM MEMBER WHERE YEAR(DateOfBirth) < '"
								+ (Integer.parseInt(year) - 50)
								+ "'"
								+ " AND MembershipType = 'Vanlig medlem' AND "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1) + "') AS above");

				if (resultSet.next()) {
					v3[0][1] = " " + resultSet.getString(1);
					v3[0][2] = " " + resultSet.getString(2);
					v3[0][3] = " " + resultSet.getString(3);
					v3[0][4] = " " + resultSet.getString(4);
					v3[0][5] = " " + resultSet.getString(5);

					v3[1][1] = " " + (difference[0] - resultSet.getInt(1));
					v3[1][2] = " " + (difference[1] - resultSet.getInt(2));
					v3[1][3] = " " + (difference[2] - resultSet.getInt(3));
					v3[1][4] = " " + (difference[3] - resultSet.getInt(4));
					v3[1][5] = " " + (difference[4] - resultSet.getInt(5));

				}

				// get age difference based on gender

				resultSet = statement
						.executeQuery("SELECT under.eighteen, eighteen.twentyfive, twentysix.forty, fortyone.fifty, above.fifty FROM "
								+ "(SELECT COUNT(*) AS eighteen FROM MEMBER WHERE YEAR(DateOfBirth) > '"
								+ (Integer.parseInt(year) - 18)
								+ "'"
								+ " AND Gender = 'Kvinne' AND "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "') AS under, "
								+ "(SELECT COUNT(*) AS twentyfive FROM MEMBER WHERE YEAR(DateOfBirth) >= '"
								+ (Integer.parseInt(year) - 25)
								+ "' AND "
								+ "YEAR(DateOfBirth) < '"
								+ (Integer.parseInt(year) - 19)
								+ "'"
								+ " AND Gender = 'Kvinne' AND "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "') AS eighteen, "
								+ "(SELECT COUNT(*) AS forty FROM MEMBER WHERE YEAR(DateOfBirth) >= '"
								+ (Integer.parseInt(year) - 40)
								+ "' AND "
								+ "YEAR(DateOfBirth) < '"
								+ (Integer.parseInt(year) - 27)
								+ "'"
								+ " AND Gender = 'Kvinne' AND "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "') AS twentysix, "
								+ "(SELECT COUNT(*) AS fifty FROM MEMBER WHERE YEAR(DateOfBirth) >= '"
								+ (Integer.parseInt(year) - 50)
								+ "' AND "
								+ "YEAR(DateOfBirth) < '"
								+ (Integer.parseInt(year) - 42)
								+ "'"
								+ " AND Gender = 'Kvinne' AND "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "') AS fortyone, "
								+ "(SELECT COUNT(*) AS fifty FROM MEMBER WHERE YEAR(DateOfBirth) < '"
								+ (Integer.parseInt(year) - 50)
								+ "'"
								+ " AND Gender = 'Kvinne' AND "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1) + "') AS above");

				if (resultSet.next()) {
					v4[1][1] = " " + resultSet.getString(1);
					v4[1][2] = " " + resultSet.getString(2);
					v4[1][3] = " " + resultSet.getString(3);
					v4[1][4] = " " + resultSet.getString(4);
					v4[1][5] = " " + resultSet.getString(5);

					v4[0][1] = " " + (difference[0] - resultSet.getInt(1));
					v4[0][2] = " " + (difference[1] - resultSet.getInt(2));
					v4[0][3] = " " + (difference[2] - resultSet.getInt(3));
					v4[0][4] = " " + (difference[3] - resultSet.getInt(4));
					v4[0][5] = " " + (difference[4] - resultSet.getInt(5));
				}

				// get youngest and oldest member

				// get youngest ord.member
				resultSet = statement
						.executeQuery("SELECT * FROM MEMBER WHERE YEAR(DateOfBirth) = "
								+ "(SELECT YEAR(DateOfBirth) FROM MEMBER WHERE "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "' AND "
								+ "MembershipType = 'Vanlig medlem' "
								+ "ORDER BY DateOfBirth DESC LIMIT 1) "
								+ "ORDER BY DateOfBirth ASC LIMIT 1");

				if (resultSet.next()) {
					v5[0][1] = " "
							+ resultSet.getString("Firstname")
							+ " "
							+ resultSet.getString("Surname")
							+ " ("
							+ (Integer.parseInt(year) - Integer
									.parseInt(resultSet
											.getString("DateOfBirth")
											.substring(0, 4))) + ") ";
				}

				// get youngest support member
				resultSet = statement
						.executeQuery("SELECT * FROM MEMBER WHERE YEAR(DateOfBirth) = "
								+ "(SELECT YEAR(DateOfBirth) FROM MEMBER WHERE "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "' AND "
								+ "MembershipType != 'Vanlig medlem' "
								+ "ORDER BY DateOfBirth DESC LIMIT 1) "
								+ "ORDER BY DateOfBirth ASC LIMIT 1");

				if (resultSet.next()) {
					v5[1][1] = " "
							+ resultSet.getString("Firstname")
							+ " "
							+ resultSet.getString("Surname")
							+ " ("
							+ (Integer.parseInt(year) - Integer
									.parseInt(resultSet
											.getString("DateOfBirth")
											.substring(0, 4))) + ") ";
				}

				// get oldest ord.member
				resultSet = statement
						.executeQuery("SELECT * FROM MEMBER WHERE YEAR(DateOfBirth) = "
								+ "(SELECT YEAR(DateOfBirth) FROM MEMBER WHERE "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "' AND "
								+ "MembershipType = 'Vanlig medlem' "
								+ "ORDER BY DateOfBirth ASC LIMIT 1) "
								+ "ORDER BY DateOfBirth DESC LIMIT 1");

				if (resultSet.next()) {
					v5[0][2] = " "
							+ resultSet.getString("Firstname")
							+ " "
							+ resultSet.getString("Surname")
							+ " ("
							+ (Integer.parseInt(year) - Integer
									.parseInt(resultSet
											.getString("DateOfBirth")
											.substring(0, 4))) + ") ";
				}

				// get oldest support member
				resultSet = statement
						.executeQuery("SELECT * FROM MEMBER WHERE YEAR(DateOfBirth) = "
								+ "(SELECT YEAR(DateOfBirth) FROM MEMBER WHERE "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "' AND "
								+ "MembershipType != 'Vanlig medlem' "
								+ "ORDER BY DateOfBirth ASC LIMIT 1) "
								+ "ORDER BY DateOfBirth DESC LIMIT 1");

				if (resultSet.next()) {
					v5[1][2] = " "
							+ resultSet.getString("Firstname")
							+ " "
							+ resultSet.getString("Surname")
							+ " ("
							+ (Integer.parseInt(year) - Integer
									.parseInt(resultSet
											.getString("DateOfBirth")
											.substring(0, 4))) + ") ";
				}

				// get average age for ord member
				resultSet = statement
						.executeQuery("SELECT "
								+ year
								+ " - AVG(YEAR(DateOfBirth)) FROM MEMBER WHERE YEAR(DateOfBirth) IN "
								+ "(SELECT YEAR(DateOfBirth) FROM MEMBER WHERE "
								+ "YEAR(TerminationDate) > '" + year + "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1) + "' AND "
								+ "MembershipType = 'Vanlig medlem')");

				if (resultSet.next()) {
					v5[0][3] = " " + Math.round(resultSet.getDouble(1));
				}
				// get average age for support member
				resultSet = statement
						.executeQuery("SELECT "
								+ year
								+ " - AVG(YEAR(DateOfBirth)) FROM MEMBER WHERE YEAR(DateOfBirth) IN "
								+ "(SELECT YEAR(DateOfBirth) FROM MEMBER WHERE "
								+ "YEAR(TerminationDate) > '" + year + "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1) + "' AND "
								+ "MembershipType != 'Vanlig medlem')");

				if (resultSet.next()) {
					v5[1][3] = " " + Math.round(resultSet.getDouble(1));
				}

				// get youngest woman
				resultSet = statement
						.executeQuery("SELECT * FROM MEMBER WHERE YEAR(DateOfBirth) = "
								+ "(SELECT YEAR(DateOfBirth) FROM MEMBER WHERE "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "' AND "
								+ "Gender = 'Kvinne' "
								+ "ORDER BY DateOfBirth DESC LIMIT 1) "
								+ "ORDER BY DateOfBirth ASC LIMIT 1");

				if (resultSet.next()) {
					v6[1][1] = " "
							+ resultSet.getString("Firstname")
							+ " "
							+ resultSet.getString("Surname")
							+ " ("
							+ (Integer.parseInt(year) - Integer
									.parseInt(resultSet
											.getString("DateOfBirth")
											.substring(0, 4))) + ") ";
				}

				// get youngest man
				resultSet = statement
						.executeQuery("SELECT * FROM MEMBER WHERE YEAR(DateOfBirth) = "
								+ "(SELECT YEAR(DateOfBirth) FROM MEMBER WHERE "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "' AND "
								+ "Gender != 'Kvinne' "
								+ "ORDER BY DateOfBirth DESC LIMIT 1) "
								+ "ORDER BY DateOfBirth ASC LIMIT 1");

				if (resultSet.next()) {
					v6[0][1] = " "
							+ resultSet.getString("Firstname")
							+ " "
							+ resultSet.getString("Surname")
							+ " ("
							+ (Integer.parseInt(year) - Integer
									.parseInt(resultSet
											.getString("DateOfBirth")
											.substring(0, 4))) + ") ";
				}

				// get oldest woman
				resultSet = statement
						.executeQuery("SELECT * FROM MEMBER WHERE YEAR(DateOfBirth) = "
								+ "(SELECT YEAR(DateOfBirth) FROM MEMBER WHERE "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "' AND "
								+ "Gender = 'Kvinne' "
								+ "ORDER BY DateOfBirth ASC LIMIT 1) "
								+ "ORDER BY DateOfBirth DESC LIMIT 1");

				if (resultSet.next()) {
					v6[1][2] = " "
							+ resultSet.getString("Firstname")
							+ " "
							+ resultSet.getString("Surname")
							+ " ("
							+ (Integer.parseInt(year) - Integer
									.parseInt(resultSet
											.getString("DateOfBirth")
											.substring(0, 4))) + ") ";
				}

				// get oldest man
				resultSet = statement
						.executeQuery("SELECT * FROM MEMBER WHERE YEAR(DateOfBirth) = "
								+ "(SELECT YEAR(DateOfBirth) FROM MEMBER WHERE "
								+ "YEAR(TerminationDate) > '"
								+ year
								+ "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "' AND "
								+ "Gender != 'Kvinne' "
								+ "ORDER BY DateOfBirth ASC LIMIT 1) "
								+ "ORDER BY DateOfBirth DESC LIMIT 1");

				if (resultSet.next()) {
					v6[0][2] = " "
							+ resultSet.getString("Firstname")
							+ " "
							+ resultSet.getString("Surname")
							+ " ("
							+ (Integer.parseInt(year) - Integer
									.parseInt(resultSet
											.getString("DateOfBirth")
											.substring(0, 4))) + ") ";
				}

				// get average age for women
				resultSet = statement
						.executeQuery("SELECT "
								+ year
								+ " - AVG(YEAR(DateOfBirth)) FROM MEMBER WHERE YEAR(DateOfBirth) IN "
								+ "(SELECT YEAR(DateOfBirth) FROM MEMBER WHERE "
								+ "YEAR(TerminationDate) > '" + year + "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1) + "' AND "
								+ "Gender = 'Kvinne')");

				if (resultSet.next()) {
					v6[1][3] = " " + Math.round(resultSet.getDouble(1));
				}
				// get average age for men
				resultSet = statement
						.executeQuery("SELECT "
								+ year
								+ " - AVG(YEAR(DateOfBirth)) FROM MEMBER WHERE YEAR(DateOfBirth) IN "
								+ "(SELECT YEAR(DateOfBirth) FROM MEMBER WHERE "
								+ "YEAR(TerminationDate) > '" + year + "' AND "
								+ "YEAR(EnrollmentDate) < '"
								+ (Integer.parseInt(year) + 1) + "' AND "
								+ "Gender != 'Kvinne')");

				if (resultSet.next()) {
					v6[0][3] = " " + Math.round(resultSet.getDouble(1));
				}

				// create list of tables

				statTable[0] = new JTable(new DefaultTableModel(v1, c1)) {
					private static final long serialVersionUID = 1L;

					public boolean isCellEditable(int rowIndex, int colIndex) {
						return false; // Disallow the editing of any cell
					}
				};

				statTable[1] = new JTable(new DefaultTableModel(v2, c2)) {
					private static final long serialVersionUID = 1L;

					public boolean isCellEditable(int rowIndex, int colIndex) {
						return false; // Disallow the editing of any cell
					}
				};

				statTable[2] = new JTable(new DefaultTableModel(v3, c3)) {
					private static final long serialVersionUID = 1L;

					public boolean isCellEditable(int rowIndex, int colIndex) {
						return false; // Disallow the editing of any cell
					}
				};

				statTable[3] = new JTable(new DefaultTableModel(v4, c4)) {
					private static final long serialVersionUID = 1L;

					public boolean isCellEditable(int rowIndex, int colIndex) {
						return false; // Disallow the editing of any cell
					}
				};

				statTable[4] = new JTable(new DefaultTableModel(v5, c5)) {
					private static final long serialVersionUID = 1L;

					public boolean isCellEditable(int rowIndex, int colIndex) {
						return false; // Disallow the editing of any cell
					}
				};

				statTable[5] = new JTable(new DefaultTableModel(v6, c6)) {
					private static final long serialVersionUID = 1L;

					public boolean isCellEditable(int rowIndex, int colIndex) {
						return false; // Disallow the editing of any cell
					}
				};

				return statTable;

			}// end try
			catch (SQLException sqlException) {
				msg.messageDialog("Kunne ikke hente statistikk", 0);
				initializeDatabase();
				return null;
			}// end catch
		}
	}

	public JTable[] getActivityStatistic(String year, FontMetrics fontmetrics) {
		if (year.length() == 0) {
			msg.messageDialog("* �rstall m� fylles ut", 2);
			return null;
		} else if (!validNr(year)) {
			msg.messageDialog("* �rstall m� inneholde 4 siffer", 2);
			return null;
		} else if (Integer.parseInt(year) > Integer.parseInt(getCurrentDate()
				.substring(6))) {
			msg.messageDialog("* Kan bare hente statistikk frem til "
					+ getCurrentDate().substring(6), 2);
			return null;
		} else {
			JTable[] statTable = new JTable[3];
			String[] c0 = { " " };
			String[][] v0 = new String[3][1];

			String[] c1 = { " ", " St�rst ", " Minst ", " Snitt " };
			String[][] v1 = new String[3][4];
			v1[0][0] = " Kapasitet";
			v1[1][0] = " Ettersp�rsel";
			v1[2][0] = " Oppm�te";

			String[] c2 = { " ID ", " Navn ", "Ansvarshavende", " Adresse ",
					" Varighet ", "Plass/Ettersp�rsel/Oppm�te(Snitt)" };
			String[][] v2 = null;

			try {
				// get all new/terminatet/total activitystatus
				resultSet = statement
						.executeQuery("SELECT count(*), neew.ac, term.ac FROM ACTIVITY, "
								+ "(SELECT count(*) AS ac FROM ACTIVITY WHERE "
								+ "YEAR(StartDate) = '"
								+ year
								+ "') AS neew, "
								+ "(SELECT count(*) AS ac FROM ACTIVITY WHERE "
								+ "YEAR(EndDate) = '"
								+ year
								+ "') AS term "
								+ "WHERE YEAR(StartDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "' AND "
								+ "YEAR(EndDate) > '"
								+ (Integer.parseInt(year) - 1) + "'");

				if (resultSet.next()) {
					v0[0][0] = " " + resultSet.getString(1);
					v0[1][0] = " " + resultSet.getString(2);
					v0[2][0] = " " + resultSet.getString(3);
				}

				// get capasity
				resultSet = statement
						.executeQuery("SELECT MAX(TotalSeats), MIN(TotalSeats), AVG(TotalSeats) FROM ACTIVITY "
								+ "WHERE YEAR(StartDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "' AND "
								+ "YEAR(EndDate) > '"
								+ (Integer.parseInt(year) - 1) + "'");

				if (resultSet.next()) {
					v1[0][1] = " "
							+ (resultSet.getString(1) == null ? 0 : resultSet
									.getString(1));
					v1[0][2] = " "
							+ (resultSet.getString(2) == null ? 0 : resultSet
									.getString(2));
					v1[0][3] = " " + Math.round(resultSet.getDouble(3));
				}

				// get demand

				resultSet = statement
						.executeQuery("SELECT MAX(demand.nr), MIN(demand.nr), AVG(demand.nr) FROM "
								+ "(SELECT COUNT(*) AS nr FROM REGISTRATION WHERE ActivityID IN "
								+ "(SELECT ActivityID FROM ACTIVITY WHERE YEAR(StartDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "' AND "
								+ "YEAR(EndDate) > '"
								+ (Integer.parseInt(year) - 1)
								+ "') GROUP BY ActivityID) AS demand");

				if (resultSet.next()) {
					v1[1][1] = " "
							+ (resultSet.getString(1) == null ? 0 : resultSet
									.getString(1));
					v1[1][2] = " "
							+ (resultSet.getString(2) == null ? 0 : resultSet
									.getString(2));
					v1[1][3] = " " + Math.round(resultSet.getDouble(3));
				}

				// get attendance
				resultSet = statement
						.executeQuery("SELECT MAX(att.stat), MIN(att.stat), AVG(att.stat) FROM "
								+ "(SELECT Total AS stat FROM ATTENDANCE WHERE ActivityID IN "
								+ "(SELECT ActivityID FROM ACTIVITY WHERE YEAR(StartDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "' AND "
								+ "YEAR(EndDate) > '"
								+ (Integer.parseInt(year) - 1) + "')) AS att");

				if (resultSet.next()) {
					v1[2][1] = " "
							+ (resultSet.getString(1) == null ? 0 : resultSet
									.getString(1));
					v1[2][2] = " "
							+ (resultSet.getString(2) == null ? 0 : resultSet
									.getString(2));
					v1[2][3] = " " + Math.round(resultSet.getDouble(3));
				}

				// get activity details
				resultSet = statement
						.executeQuery("SELECT * FROM ACTIVITY WHERE YEAR(StartDate) < '"
								+ (Integer.parseInt(year) + 1)
								+ "' AND "
								+ "YEAR(EndDate) > '"
								+ (Integer.parseInt(year) - 1) + "'");
				if (resultSet.next()) {
					resultSet.last();
					v2 = new String[resultSet.getRow()][6];
					resultSet.beforeFirst();
					int x = 0;
					while (resultSet.next()) {
						v2[x][0] = " " + resultSet.getString("ActivityID");
						v2[x][1] = " " + resultSet.getString("Name");
						v2[x][2] = " " + resultSet.getString("Manager");
						v2[x][3] = " " + resultSet.getString("Address");
						v2[x][4] = " "
								+ outputDateFormat(resultSet
										.getString("StartDate"))
								+ " - "
								+ outputDateFormat(resultSet
										.getString("EndDate"));
						v2[x++][5] = " " + resultSet.getString("TotalSeats")
								+ " / 0 / 0";

					}
					// get demand/attendance
					for (x = x - 1; x >= 0; x--) {
						resultSet = statement
								.executeQuery("SELECT demand.nr, AVG(att.stat) FROM "
										+ "(SELECT COUNT(*) AS nr FROM REGISTRATION WHERE ActivityID = '"
										+ v2[x][0]
										+ "') AS demand, "
										+ "(SELECT Total AS stat FROM ATTENDANCE WHERE ActivityID = '"
										+ v2[x][0] + "') AS att");
						if (resultSet.next()) {
							v2[x][5] = v2[x][5].substring(0,
									v2[x][5].length() - 8)
									+ " / "
									+ (resultSet.getString(1) == null ? 0
											: resultSet.getString(1))
									+ " / "
									+ (resultSet.getString(2) == null ? 0
											: Math.round(resultSet.getDouble(2)));
						}
					}
				} else {
					c2 = new String[1];
					c2[0] = " ";
					v2 = new String[1][1];
					v2[0][0] = " Ingen aktiviteter dette �ret";
				}

				// create list of tables

				statTable[0] = new JTable(new DefaultTableModel(v0, c0)) {
					private static final long serialVersionUID = 1L;

					public boolean isCellEditable(int rowIndex, int colIndex) {
						return false; // Disallow the editing of any cell
					}
				};

				statTable[1] = new JTable(new DefaultTableModel(v1, c1)) {
					private static final long serialVersionUID = 1L;

					public boolean isCellEditable(int rowIndex, int colIndex) {
						return false; // Disallow the editing of any cell
					}
				};

				statTable[2] = new JTable(new DefaultTableModel(v2, c2)) {
					private static final long serialVersionUID = 1L;

					public boolean isCellEditable(int rowIndex, int colIndex) {
						return false; // Disallow the editing of any cell
					}
				};
			} catch (SQLException sqlException) {
				sqlException.printStackTrace();
				msg.messageDialog("Kunne ikke hente statistikk", 0);
				initializeDatabase();
				return null;
			}

			return statTable;
		}
	}

	public String[] getMemberList() {
		try {
			resultSet = statement
					.executeQuery("SELECT MemberID, Firstname, Surname FROM MEMBER WHERE "
							+ "TerminationDate > '"
							+ sqlDateFormat(getCurrentDate()) + "'");

			if (resultSet.next()) {
				resultSet.last();
				String[] list = new String[resultSet.getRow()];
				resultSet.first();
				for (int i = 0; i < list.length; i++) {
					list[i] = resultSet.getString("MemberID") + " "
							+ resultSet.getString("Firstname") + " "
							+ resultSet.getString("Surname");
					resultSet.next();
				}
				return list;
			} else {
				msg.messageDialog(
						"Det finnes ingen medlemmer\nsom kan meldes opp til aktiviteten",
						2);
				return null;
			}
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
			msg.messageDialog("Kunne ikke hente medlemsliste", 0);
			initializeDatabase();
			return null;
		}
	}

	public boolean addRegistration(String member, String activity) {
		String[] tmp = activity.split(" : ");
		StringBuilder s = new StringBuilder();
		for (int i = 1; i < tmp.length; i++)
			s.append(tmp[i]);
		try {
			resultSet = statement
					.executeQuery("SELECT * FROM REGISTRATION WHERE "
							+ "ActivityID = '" + activity.split(" ")[0]
							+ "' AND " + "MemberID = '" + member.split(" ")[0]
							+ "'");
			if (resultSet.next()) {
				msg.messageDialog("Medlemmet er allerede oppmeldt!", 2);
				return false;
			}
			statement.executeUpdate("BEGIN");
			statement.executeUpdate("INSERT INTO REGISTRATION VALUES (" + "'"
					+ activity.split(" ")[0] + "', " + "'"
					+ member.split(" ")[0] + "')");

			addMemberLog(member.split(" ")[0],
					"Meldt opp til \"" + s.toString() + "\"");
			tmp = member.split(" ");
			s = new StringBuilder();
			for (int i = 1; i < tmp.length; i++) {
				s.append(tmp[i]);
				s.append(" ");
			}

			addActivityLog(activity.split(" ")[0], s.toString()
					+ "har meldt seg opp");
			statement.executeUpdate("COMMIT");
			msg.messageDialog(s.toString() + "er meldt opp", 1);
			return true;
		} // end try
		catch (SQLException sqlException) {
			msg.messageDialog("Kunne ikke fullf�re oppmelding", 0);
			initializeDatabase();
			return false;
		}// end catch
	}

	public String[] getInterestList(String activityID) {
		// query database
		try {
			resultSet = statement
					.executeQuery("SELECT R.MemberID, M.Firstname, M.Surname FROM REGISTRATION AS R, MEMBER AS M WHERE "
							+ "ActivityID = '"
							+ activityID
							+ "' AND "
							+ "M.MemberID = R.MemberID");

			// process query results
			if (resultSet.next()) {
				resultSet.last();
				String[] list = new String[resultSet.getRow()];
				resultSet.first();
				for (int i = 0; i < list.length; i++) {
					list[i] = resultSet.getString("MemberID") + " "
							+ resultSet.getString("Firstname") + " "
							+ resultSet.getString("Surname");
					resultSet.next();
				}
				return list;
			} else {
				msg.messageDialog(
						"Det finnes ingen medlemmer\nsom er meldt opp til aktiviteten",
						2);
				return null;
			}
		} // end try
		catch (SQLException sqlException) {
			msg.messageDialog("Kunne ikke hente medlemsliste", 0);
			initializeDatabase();
			return null;
		}// end catch
	}

	public boolean removeRegistration(String member, String activity) {
		String[] tmp = activity.split(" : ");
		StringBuilder s = new StringBuilder();
		for (int i = 1; i < tmp.length; i++)
			s.append(tmp[i]);
		// query database
		try {
			resultSet = statement
					.executeQuery("SELECT * FROM REGISTRATION WHERE "
							+ "ActivityID = '" + activity.split(" ")[0]
							+ "' AND " + "MemberID = '" + member.split(" ")[0]
							+ "'");

			// process query results
			if (!resultSet.next()) {
				msg.messageDialog("Medlemmet er allerede avmeldt!", 2);
				return false;
			}

			// query database
			statement.executeUpdate("BEGIN");
			statement.executeUpdate("DELETE FROM REGISTRATION WHERE "
					+ "ActivityID = '" + activity.split(" ")[0] + "' AND "
					+ "MemberID = '" + member.split(" ")[0] + "'");

			addMemberLog(member.split(" ")[0], "Avmeldt fra \"" + s.toString()
					+ "\"");
			tmp = member.split(" ");
			s = new StringBuilder();
			for (int i = 1; i < tmp.length; i++) {
				s.append(tmp[i]);
				s.append(" ");
			}

			addActivityLog(activity.split(" ")[0], s.toString()
					+ "har meldt seg av");
			statement.executeUpdate("COMMIT");
			msg.messageDialog(s.toString() + "er avmeldt", 1);
			return true;
		} // end try
		catch (SQLException sqlException) {
			msg.messageDialog("Kunne ikke fullf�re avmelding", 0);
			initializeDatabase();
			return false;
		}// end catch
	}

	public String[] getM2Alist(String memberID) {
		// query database
		try {
			resultSet = statement
					.executeQuery("SELECT R.ActivityID, A.Name FROM REGISTRATION AS R, ACTIVITY AS A WHERE "
							+ "MemberID = '"
							+ memberID
							+ "' AND "
							+ "A.ActivityID = R.ActivityID");

			// process query results
			if (resultSet.next()) {
				resultSet.last();
				String[] list = new String[resultSet.getRow()];
				resultSet.first();
				for (int i = 0; i < list.length; i++) {
					list[i] = resultSet.getString("ActivityID") + " "
							+ resultSet.getString("Name");
					resultSet.next();
				}
				return list;
			} else {
				msg.messageDialog(
						"Medlemmet er ikke oppmeldt til noen aktivitet", 2);
				return null;
			}
		} // end try
		catch (SQLException sqlException) {
			msg.messageDialog("Kunne ikke hente aktivitetsliste", 0);
			initializeDatabase();
			return null;
		}// end catch
	}
}