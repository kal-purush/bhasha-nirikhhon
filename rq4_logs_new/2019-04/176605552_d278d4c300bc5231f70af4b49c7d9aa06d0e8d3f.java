package extraction.sProject;

import java.sql.Statement;

public class ETMCrud {
	
	public void insertETM(Statement st, String module_name, double score, String empId){
		try {
			System.out.println("Inserting a modle scores...");
			int rows = st.executeUpdate("INSERT INTO employees_take_modules VALUES((Select module_id FROM modules WHERE module_name Like '"+ module_name+"'), '"+ empId + "', "+ score +")" );
			System.out.println(rows + " module added...");
		} catch (Exception e) {
			System.out.println("Exception " + e.getMessage());
		}
	}

}