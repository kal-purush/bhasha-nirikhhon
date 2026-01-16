package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import application.DateTime;
import application.TaskData;

public class Database {
	private static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String DATABASE_NAME = "TaskManagerDatabase";
	private static final String CONNECTION_URL = "jdbc:derby:" + DATABASE_NAME + ";create=true";
	private static final String COMMAND_CREATE_TABLE = "create table TaskList(Task_ID varchar(5), Task_Type varchar(20), "
			+ "Description varchar(60), Start_Date_Time varchar(20), End_Date_Time varchar(20), Deadline varchar(20), Status varchar(20))";
	private static final String SEARCH_KEYWORD = "select $s from TaskList";
	private static final String[] headings = {
		"Task ID: ", 
		"Task type: ",
		"Description: ",
		"Start Date Time: ",
		"End Date Time: ",
		"Deadline: ",
		"Status: "
	};
	private static final String COMMAND_ADD = "insert into %s values ('%s', '%s', '%s', '%s', '%s', '%s', '%s')";
	
	
	public static void createDatabase() {
		Connection connection = null;
		
		try {
			connection = DriverManager.getConnection(CONNECTION_URL);
			System.out.println("Connected to " + DATABASE_NAME);
			connection.createStatement().execute(COMMAND_CREATE_TABLE);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("Database is successfully created!");
	}
	
	public static void queryDatabase() {		
		try {
			Connection connection = DriverManager.getConnection(CONNECTION_URL);
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery("SELECT * FROM TaskList");
			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			int columnCount = resultSetMetaData.getColumnCount();
			
			
			while (resultSet.next()) {
				System.out.println("");
				for (int x = 1; x <= columnCount; x++) {
					System.out.format("%20s", resultSet.getString(x));
					System.out.println();
				}
			}
			if (statement != null) {
				statement.close();
			}
			if (connection != null) {
				connection.close(); 
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private static String getAddCommand(TaskData taskData) {
		String taskType = taskData.getTaskType();
		String description = taskData.getDescription();
		
		String startDateTime;
		if (taskData.getStartDateTime() != null) {
			startDateTime = taskData.getStartDateTime().formatDateTime();
		} else {
			startDateTime = "n.a.";
		}
		
		String endDateTime;
		if (taskData.getEndDateTime() != null) {
			endDateTime = taskData.getEndDateTime().formatDateTime();
		} else {
			endDateTime = "n.a.";
		}
		
		String deadline;
		if (taskData.getDeadLine() != null) {
			deadline = taskData.getDeadLine().formatDateTime();
		} else {
			deadline = "n.a.";
		}
		String status = taskData.getStatus();
		String taskID = taskData.getTaskID();
		
		String addCommand = String.format(COMMAND_ADD, "TaskList", taskID, taskType, description, startDateTime, endDateTime, deadline, status);
		
		return addCommand;
	}
	
	public static void addData(TaskData taskData) {
		String addCommand = getAddCommand(taskData);
		
		try {
			Connection connection = DriverManager.getConnection(CONNECTION_URL);
			connection.createStatement().execute(addCommand);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println("Data added successfully!");
	}
	
	public static void clearDatabase() {
		
	}
	
	public static void shutDownDatabase() {
		
	}
	
	public boolean isDatabaseCreated() {
		try {
			Connection connection = DriverManager.getConnection(CONNECTION_URL);
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery("select * from TaskList");
		} catch (SQLException e) {
			return false;
		}
		return true;
	}

	public static void main (String[] args) throws SQLException {
		//Database.createDatabase();
		
		DateTime deadline1 = new DateTime(1993,01,07,5,41);
		TaskData data1 = new TaskData("1", "deadline", "data1", null, null, deadline1, "uncompleted"); 

		DateTime deadline2 = new DateTime(1993,02,17,5,41);
		TaskData data2 = new TaskData("2", "deadline", "data2", null, null, deadline2, "uncompleted");

		DateTime deadline3 = new DateTime(1993,04,04,5,41);
		TaskData data3 = new TaskData("3", "deadline", "dummy", null, null, deadline3, "uncompleted");

		Database.addData(data3);
		Database.addData(data2);
		Database.addData(data1);

		Database.queryDatabase();
	}

}