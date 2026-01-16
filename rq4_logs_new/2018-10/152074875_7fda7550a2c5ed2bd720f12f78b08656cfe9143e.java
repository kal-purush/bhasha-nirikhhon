package employee_stuff;
import java.sql.*;

public class MySQLHandler {
	
	private String _Hostname;
	private String _Username;
	private String _Password;
	
	Connection C;
	
	
	
	
	public MySQLHandler() {
		
	}
	
	public MySQLHandler(String Hostname, String Username, String Password) {
		Connect(Hostname, Username, Password);
	}
	
	public void Connect(String Hostname, String Username, String Password) {
		set_Hostname(Hostname);
		set_Username(Username);
		set_Password(Password);
		
		 try {
			this.C = DriverManager.getConnection(Hostname,
					Username, Password);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public ResultSet Query(String Quer) {
		Statement st;
		try {
			if(C.isClosed()) {
				Connect(this._Hostname,this._Username, this._Password)
			}
			st = C.createStatement();
	
			
		
		
		ResultSet rs;
		
			rs = st.executeQuery(Quer);
			return rs;
		}
		 catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
		
		
	}
	
	public void Disconnect() throws SQLException {
		C.close();
	}
	
	
		
	
	

	
	private String get_Hostname() {
		return _Hostname;
	}
	private void set_Hostname(String _Hostname) {
		this._Hostname = _Hostname;
	}
	private String get_Username() {
		return _Username;
	}
	private void set_Username(String _Username) {
		this._Username = _Username;
	}
	private String get_Password() {
		return _Password;
	}
	private void set_Password(String _Password) {
		this._Password = _Password;
	}
}