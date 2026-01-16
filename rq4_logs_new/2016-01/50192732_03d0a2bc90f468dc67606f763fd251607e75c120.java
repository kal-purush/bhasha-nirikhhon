package com.cdia.data.domain;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

@Embeddable
public class Contacto {
	
	private String nombrs;	
	private String tel;
	private String cel;	
	private String direc;
	private Pais PaisResid;
	private Departamento deptoResid;
	private Ciudad ciudadResid;
	
	public Contacto() { }
	
	@Column(name = "NOMBRE")
	public String getNombrs(){
		return nombrs;
	}
	
	public void setNombrs(String nombrs) {
		this.nombrs = nombrs;
	}
	
	@Column(name = "TELEFONO")
	public String getTel(){
		return tel;
	}
	
	public void setTel(String tel) {
		this.tel = tel;
	}
	
	@Column(name = "CELULAR")
	public String getCel(){
		return cel;
	}
	
	public void setCel(String cel) {
		this.cel = cel;
	}
	
	@Column(name = "DIRECCION")
	public String getDirec(){
		return direc;
	}
	
	public void setDirec(String direc) {
		this.direc = direc;
	}
	
	@ManyToOne
	@JoinColumn(name="CODPAISRES")
	public Pais getPaisResid(){
		return PaisResid;
	}
	
	public void setPaisResid(Pais paisResid) {
		PaisResid = paisResid;
	}
	
	@ManyToOne
	@JoinColumn(name="CODDPTORES")
	public Departamento getDeptoResid(){
		return deptoResid;
	}
	
	public void setDeptoResid(Departamento deptoResid) {
		this.deptoResid = deptoResid;
	}
	
	@ManyToOne
	@JoinColumn(name="CODCIUDRES")
	public Ciudad getCiudadResid(){
		return ciudadResid;
	}	
	
	public void setCiudadResid(Ciudad ciudadResid) {
		this.ciudadResid = ciudadResid;
	}

	@Override
	public String toString() {
		return "Nombres:"+nombrs+" direccion:"+direc+" ciudad residencia: "+ciudadResid;
	}
	
	

}