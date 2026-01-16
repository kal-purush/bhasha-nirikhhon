package se.tennander.family.member;

import com.fasterxml.jackson.annotation.JsonProperty;
import se.tennander.family.member.storage.Member;

/**
 * Header used to render a small representation of the member suitable when
 * viewing multiple users.
 */
class MemberHeaderView {

	@JsonProperty final int id;
	@JsonProperty final String givenName;
	@JsonProperty final String surName;

	MemberHeaderView(int id, String givenName, String surName) {
		this.id = id;
		this.givenName = givenName;
		this.surName = surName;
	}

  /**
   * Creates a {@link MemberHeaderView} from a {@link Member} object.
   * @param member The member to be represented.
   * @return a header view of that member.
   */
	static MemberHeaderView create(Member member) {
		return new MemberHeaderView(member.id, member.givenName, member.surName);
	}
}