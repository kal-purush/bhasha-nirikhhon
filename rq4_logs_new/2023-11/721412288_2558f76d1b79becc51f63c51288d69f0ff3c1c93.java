package com.smyr.showmeyourrecipe.entity.user;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.*;
import org.springframework.security.crypto.password.PasswordEncoder;

@Entity
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class EmailAuth {

	@Id
	private String id;
	private String username;
	private String password;
	private String introduce;
	private String email;

	@Builder
	public EmailAuth( String id, String username, String password, String introduce, String email ) {
		this.id = id;
		this.username = username;
		this.password = password;
		this.introduce = introduce;
		this.email = email;
	}
}