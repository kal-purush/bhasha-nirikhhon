package extraction.sProject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ClassCRUD {

	public void insertClass(Statement st, String class_id, String stream_id, String user_id){
		try {
			System.out.println("Inserting a class...");
			int rows = st.executeUpdate("INSERT INTO Student_Performance.Class VALUES ('" + class_id + "','" + stream_id + "','" + user_id  + "')");
			System.out.println(rows + " class added...");
		} catch (Exception e) {
			System.out.println("Exception " + e.getMessage());
		}
	}
	
	public void readClass(Statement st, String class_id){
		try {
			System.out.println("Reading class...");
			ResultSet rs = st.executeQuery("SELECT class_id, stream_id, user_id FROM Student_Performance.Class WHERE class_id = '" + class_id + "'");
			while(rs.next())
				System.out.println("Result: " + rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
		} catch (Exception e) {
			System.out.println("Exception " + e.getMessage());
		}
	}
	
	public void updateClass(Statement st, String class_id, String stream_id, String user_id){
		try {
			System.out.println("Update class.");
			int rows = st.executeUpdate("UPDATE Student_Performance.Class SET stream_id ='" + stream_id + "', user_id='" + user_id + "'WHERE class_id ='" + class_id + "'");
			System.out.println(rows + " class updated");
		} catch (Exception e) {
			System.out.println("Exception " + e.getMessage());
		}
	}
	
	public void deleteClass(Statement st, String class_id){
		try {
			System.out.println("Delete class.");
			int rows = st.executeUpdate("DELETE FROM Student_Performance.Class WHERE class_id = '" + class_id + "'");
			System.out.println(rows + " class deleted");
		} catch (Exception e) {
			System.out.println("Exception " + e.getMessage());
		}
	}

}