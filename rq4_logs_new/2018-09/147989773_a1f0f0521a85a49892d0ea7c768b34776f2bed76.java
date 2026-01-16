package hal_shop.util.errors;

public class Errors {

	private String code = "";
	private String message = "";

	public Errors Exception404() {
		this.code = "AuthError";
		this.message = "ログインを行なってください。";
		return this;
	}
	
	public Errors CreateException(String code, String message) {
		this.code = code;
		this.message = message;
		return this;
	}

	@Override
	public String toString() {
		return "Errors [code=" + code + ", message=" + message + "]";
	}

	public String toJSON() {
		return "{" + "\"" + "code" + "\":" + "\"" + this.code + "\"," + "\"" + "message" + "\":" + "\"" + this.message
				+ "\"" + "}";
	}
}