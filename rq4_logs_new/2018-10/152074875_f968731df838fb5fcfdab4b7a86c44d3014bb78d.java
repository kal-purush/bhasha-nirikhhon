package app;
import java.sql.*;

public class MySQLHandler {
	
	private String _Hostname;
	private String _Username;
	private String _Password;
	
	Connection C;
	public Boolean Connected = false;
	public MySQLHandler() {
	}
	
	public MySQLHandler(String Hostname, String Username, String Password) throws SQLException {
		Connect(Hostname, Username, Password);
	}
	
	public void Connect(String Hostname, String Username, String Password)
	throws SQLException{
		set_Hostname(Hostname);
		set_Username(Username);
		set_Password(Password);
		 try {
			this.C = DriverManager.getConnection(Hostname,
					Username, Password);
			Connected = true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
			//System.out.println("1 is "+e.getErrorCode());
			//e.printStackTrace();
			Connected = false;
		}
	}
	
	public void Statement(String Statement) throws SQLException {
		try {
			if(C.isClosed()) {
				Connect(this._Hostname,this._Username, this._Password);
			}
			Statement st = C.createStatement();
			st.executeUpdate(Statement);
			
		} finally {}
	}
	
	public ResultSet Query(String Quer) {
		Statement st;
		try {
			if(C.isClosed()) {
				Connect(this._Hostname,this._Username, this._Password);
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
		Connected = false;
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