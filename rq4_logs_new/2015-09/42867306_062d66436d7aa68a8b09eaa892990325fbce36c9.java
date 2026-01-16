package com.voiturier.web;

public class VueEtablissementBean {

	public VueEtablissementBean() {
		// TODO Auto-generated constructor stub
	}

	private String raisonSocial;
	private String type;
	private String numerotelephone;
	private String siteweb;
	private String contact;
	private String email;
	private String fonction;
	private String motdepasse;
	private String adresse;
	private String code_postal;
	private String ville;
	private String Siret;
	private Integer Service_idService;

	/**
	 * @return the service_idService
	 */
	public Integer getService_idService() {
		return Service_idService;
	}

	/**
	 * @param service_idService
	 *            the service_idService to set
	 */
	public void setService_idService(Integer service_idService) {
		Service_idService = service_idService;
	}

	public String getRaisonSocial() {
		return this.raisonSocial;
	}

	public VueEtablissementBean(String raisonSocial, String type, String numerotelephone, String siteweb,
			String contact, String email, String fonction, String motdepasse, String adresse, String code_postal,
			String ville, String siret, Integer Service_idService) {
		super();
		this.raisonSocial = raisonSocial;
		this.type = type;
		this.numerotelephone = numerotelephone;
		this.siteweb = siteweb;
		this.contact = contact;
		this.email = email;
		this.fonction = fonction;
		this.motdepasse = motdepasse;
		this.adresse = adresse;
		this.code_postal = code_postal;
		this.ville = ville;
		this.Siret = siret;
		this.Service_idService = Service_idService;
	}

	public void setRaisonSocial(String raisonSocial) {
		this.raisonSocial = raisonSocial;
	}

	public String getType() {
		return this.type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getNumerotelephone() {
		return this.numerotelephone;
	}

	public void setNumerotelephone(String numerotelephone) {
		this.numerotelephone = numerotelephone;
	}

	public String getSiteweb() {
		return this.siteweb;
	}

	public void setSiteweb(String siteweb) {
		this.siteweb = siteweb;
	}

	public String getContact() {
		return this.contact;
	}

	public void setContact(String contact) {
		this.contact = contact;
	}

	public String getEmail() {
		return this.email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getFonction() {
		return this.fonction;
	}

	public void setFonction(String fonction) {
		this.fonction = fonction;
	}

	public String getMotdepasse() {
		return this.motdepasse;
	}

	public void setMotdepasse(String motdepasse) {
		this.motdepasse = motdepasse;
	}

	public String getAdresse() {
		return this.adresse;
	}

	public void setAdresse(String adresse) {
		this.adresse = adresse;
	}

	public String getCode_postal() {
		return this.code_postal;
	}

	public void setCode_postal(String code_postal) {
		this.code_postal = code_postal;
	}

	public String getVille() {
		return this.ville;
	}

	public void setVille(String ville) {
		this.ville = ville;
	}

	public String getSiret() {
		return this.Siret;
	}

	public void setSiret(String siret) {
		this.Siret = siret;
	}

}