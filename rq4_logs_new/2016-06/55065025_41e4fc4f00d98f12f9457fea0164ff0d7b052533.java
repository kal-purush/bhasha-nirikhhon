package es.unizar.tmdad.model;

import java.util.List;

public class ThemeDAO {
	private long id = -1;
	private String title;
	private List<String> tokens;
	
	public ThemeDAO(){}
	
	public ThemeDAO(long id, String title, List<String> tokens){
		this.id = id;
		this.title = title;
		this.tokens = tokens;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public List<String> getTokens() {
		return tokens;
	}

	public void setTokens(List<String> tokens) {
		this.tokens = tokens;
	}
}