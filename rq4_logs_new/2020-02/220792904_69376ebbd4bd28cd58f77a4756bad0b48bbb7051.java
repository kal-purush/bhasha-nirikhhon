package main.dto;

import main.model.ZahtevZaOdmor;

public class PrihodiDTO {
	
	private Long id;

	private String start;

	private String end;
	
	
	public PrihodiDTO() {
		
	}
	
	public PrihodiDTO(Long id, String start, String end) {
		super();
		this.start = start;
		this.end = end;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getStart() {
		return start;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public String getEnd() {
		return end;
	}

	public void setEnd(String end) {
		this.end = end;
	}
	
	


}