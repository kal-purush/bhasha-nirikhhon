/**
 * 
 */
package com.yn.cfer.community.model;

import com.google.gson.annotations.SerializedName;

/**
 * @author user
 *
 */
public class Picture {
	@SerializedName("dynamics_id")
	private Integer dynamicsId;
	@SerializedName("member_id")
	private Integer memberId;
	private String url;
	public Integer getDynamicsId() {
		return dynamicsId;
	}
	public void setDynamicsId(Integer dynamicsId) {
		this.dynamicsId = dynamicsId;
	}
	public Integer getMemberId() {
		return memberId;
	}
	public void setMemberId(Integer memberId) {
		this.memberId = memberId;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
}