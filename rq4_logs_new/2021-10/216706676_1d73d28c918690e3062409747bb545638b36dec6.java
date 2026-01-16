package com.zone5cloud.core.terms;

import java.net.URL;

import com.google.gson.annotations.SerializedName;

/**
 * Object returned by requiredTerms, indicating the Terms and Conditions that must be accepted for
 * a particular App-Key (ClientId) before a user can log in.
 * 
 * Also returned in a login response when there are previously accepted terms that have been updated and
 * require re-acceptance
 *
 * either url or filename will be present, but not both. url is present when the document content is located
 * at an external url. filename is present when the document content is stored on the server.
 * 
 * published is only present for updated terms in a login response
 */
public class TermsAndConditions {
	private String companyId;
	
	@SerializedName(value = "termsId", alternate = "id")
	private String termsId;
	
	@SerializedName(value = "name", alternate = "displayName")
	private String name;
	
	private String filename;
	
	private Integer version;
	
	private String displayVersion;
	
	private String alias;
	
	private URL url;
	
	// Only returned with updated terms
	private Long published;
	
	public String getCompanyId() {
		return companyId;
	}
	
	public void setCompanyId(String companyId) {
		this.companyId = companyId;
	}
	
	public String getTermsId() {
		return termsId;
	}
	
	public void setTermsId(String termsId) {
		this.termsId = termsId;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String displayName) {
		this.name = displayName;
	}
	
	public String getFilename() {
		return filename;
	}
	
	public void setFilename(String filename) {
		this.filename = filename;
	}
	
	public Integer getVersion() {
		return version;
	}
	
	public void setVersion(Integer version) {
		this.version = version;
	}
	
	public String getDisplayVersion() {
		return displayVersion;
	}
	
	public void setDisplayVersion(String displayVersion) {
		this.displayVersion = displayVersion;
	}
	
	public String getAlias() {
		return alias;
	}
	
	public void setAlias(String alias) {
		this.alias = alias;
	}
	
	public URL getUrl() {
		return url;
	}
	
	public void setUrl(URL url) {
		this.url = url;
	}

	public Long getPublished() {
		return published;
	}

	public void setPublished(Long published) {
		this.published = published;
	}

}