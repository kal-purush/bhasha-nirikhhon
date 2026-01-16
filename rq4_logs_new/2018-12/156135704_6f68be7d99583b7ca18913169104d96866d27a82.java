/**
 * 
 */
package se.tennander.family.member;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author bjorn
 *
 */
public class MemberHeader {

	@JsonProperty final int id;
	@JsonProperty final String givenName;
	@JsonProperty final String surName;

	public MemberHeader(int id, String givenName, String surName) {
		this.id = id;
		this.givenName = givenName;
		this.surName = surName;
	}

	public static MemberHeader from(Member member) {
		return new MemberHeader(member.id, member.givenName, member.surName);
	}
}