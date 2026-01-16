package model.response;

import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;

public class Response {

	public Response() {
		this(null);
	}
	
	public Response(String body) {
		setBody(body);
	}
	
	private StringProperty body = new SimpleStringProperty();
	public StringProperty bodyProperty() {return body;}
	public String getBody() {return body.get();}
	public void setBody(String m) {body.set(m);}
}
