package data;

public class UserLogin {
	private int id;
	private String username;
	private String pswd;


	public UserLogin(int id, String username, String pswd) {
		this.id = id;
		this.username = username;
		this.pswd = pswd;
		
	}
	public int getID() {
		return id;
	}
	public String getUsername() {
		return username;
	}
	public String getPswd() {
		return pswd;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("User [id=").append(id)
			.append(", username=").append(username).append(", password=")
			.append(pswd).append("]");
		return builder.toString();
	}

}