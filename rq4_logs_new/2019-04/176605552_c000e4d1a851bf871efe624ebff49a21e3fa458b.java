import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

/*
 * PDF functions that John asked for to display relevent data in the 
 * report 
 * 
 */

public class PDFinfo {
    public PDFinfo(){
        
    }
	public Connection c1;
	 
	//method to get just the employee name depending on the ID that you pass in
	public String getEmployeeName(String empID) throws Exception{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			c1 = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "Student_Performance", "Student_Performance");
			Statement s1 = c1.createStatement();
			ResultSet r1=s1.executeQuery("SELECT name FROM Employees WHERE employee_id = '"+empID+"'");
			String str="";
			while(r1.next()){
				str = r1.getString(1);
			}
			r1.close();
                        s1.close();
                        c1.close();
			return str;
                        
                        
                        
		}
	
	public ArrayList<String> getStreamIDName(String empID) throws Exception{
		ArrayList<String> list = new ArrayList();
               
		Class.forName("oracle.jdbc.driver.OracleDriver");
		c1 = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "Student_Performance", "Student_Performance");
		Statement s1 = c1.createStatement();
		ResultSet r1=s1.executeQuery("select s.stream_id, s.stream_name from Stream s, Class c, Employees e where s.stream_id=c.stream_id and c.class_id=e.class_id and e.employee_id='"+empID+"'");
		while(r1.next()){
			list.add(r1.getString(1));
			list.add(r1.getString(2));
		}
              
		r1.close();
                s1.close();
                c1.close();
		return list;
	}
	
	public String getAverageScoresByEmployeeID(String empID) throws Exception{
		Class.forName("oracle.jdbc.driver.OracleDriver");
		c1 = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "Student_Performance", "Student_Performance");
		Statement s1 = c1.createStatement();
		ResultSet r1=s1.executeQuery("select avg(e.scores) from Employees_take_Modules e, Modules m where m.module_id=e.module_id and e.employee_id='"+empID+"'");
		String str = "";
		while(r1.next()){
			str = r1.getString(1);
			
		}
		r1.close();
                        s1.close();
                        c1.close();
		return str;
	}
	
	public String getAverageScoresByFoundationEmployeeID(String empID) throws Exception{
		Class.forName("oracle.jdbc.driver.OracleDriver");
		c1 = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "Student_Performance", "Student_Performance");
		Statement s1 = c1.createStatement();
		ResultSet r1=s1.executeQuery("select avg(e.scores) from Employees_take_Modules e, Modules m where m.category='Foundation' and m.module_id=e.module_id and e.employee_id='"+empID+"'");
		String str = "";
		while(r1.next()){
			str = r1.getString(1);
			
		}
		
		return str;
	}
	
	public String getAverageScoresBySpecializationEmployeeID(String empID) throws Exception{
		Class.forName("oracle.jdbc.driver.OracleDriver");
		c1 = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "Student_Performance", "Student_Performance");
		Statement s1 = c1.createStatement();
		ResultSet r1=s1.executeQuery("select avg(e.scores) from Employees_take_Modules e, Modules m where m.category='Specialization' and m.module_id=e.module_id and e.employee_id='"+empID+"'");
		String str = "";
		while(r1.next()){
			str = r1.getString(1);
			
		}
		
		return str;
	}
	
	public String getAverageScoresByProcessDomainEmployeeID(String empID) throws Exception{
		Class.forName("oracle.jdbc.driver.OracleDriver");
		c1 = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "Student_Performance", "Student_Performance");
		Statement s1 = c1.createStatement();
		ResultSet r1=s1.executeQuery("select avg(e.scores) from Employees_take_Modules e, Modules m where m.category='ProcessDomain' and m.module_id=e.module_id and e.employee_id='"+empID+"'");
		String str = "";
		while(r1.next()){
			str = r1.getString(1);
			
		}
		
		return str;
	}
	
	public ArrayList<String> getModScoreByFoundation(String empID) throws Exception{
		ArrayList<String> list = new ArrayList<String>();
		Class.forName("oracle.jdbc.driver.OracleDriver");
		c1 = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "Student_Performance", "Student_Performance");
		Statement s1 = c1.createStatement();
		ResultSet r1=s1.executeQuery("select m.module_name,e.scores from modules m, employees_take_modules e where m.module_id=e.module_id and category='Foundation' and e.employee_id='"+empID+"'");
		while(r1.next()){
			list.add(r1.getString(1));
			list.add(r1.getString(2));
			
		}
		
		return list;
	}
	
	public ArrayList<String> getModScoreBySpecialization(String empID) throws Exception{
		ArrayList<String> list = new ArrayList<String>();
		Class.forName("oracle.jdbc.driver.OracleDriver");
		c1 = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "Student_Performance", "Student_Performance");
		Statement s1 = c1.createStatement();
		ResultSet r1=s1.executeQuery("select m.module_name,e.scores from modules m, employees_take_modules e where m.module_id=e.module_id and category='Specialization' and e.employee_id='"+empID+"'");
		while(r1.next()){
			list.add(r1.getString(1));
			list.add(r1.getString(2));
			
		}
		
		return list;
	}
	
	public ArrayList<String> getModScoreByProcessDomain(String empID) throws Exception{
		ArrayList<String> list = new ArrayList<String>();
		Class.forName("oracle.jdbc.driver.OracleDriver");
		c1 = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "Student_Performance", "Student_Performance");
		Statement s1 = c1.createStatement();
		ResultSet r1=s1.executeQuery("select m.module_name,e.scores from modules m, employees_take_modules e where m.module_id=e.module_id and category='ProcessDomain' and e.employee_id='"+empID+"'");
		while(r1.next()){
			list.add(r1.getString(1));
			list.add(r1.getString(2));
		}
		
		return list;
	}
}