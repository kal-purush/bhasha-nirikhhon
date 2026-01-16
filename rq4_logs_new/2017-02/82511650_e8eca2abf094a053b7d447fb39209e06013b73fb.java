package domain;

import java.util.ArrayList;
import java.util.Collection;

import org.springframework.security.core.userdetails.UserDetails;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * 
 * @author Rob Paul Jr
 *
 */
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class AuthenticatedUser implements UserDetails {

	private static final long serialVersionUID = -1059148434551710817L;

	@Getter 
	@Setter
	private Long id;
	
	@Getter 
	@Setter
	private String sid; 

	@Getter 
	@Setter
	private Collection<Responsibilities> authorities = new ArrayList<>(); 
	
	@Getter
	@Setter
	private String subjectDn;
	
	@Getter
	@Setter
	private String issuerDn; 
	
	@Getter 
	@Setter
	private String password; 

	@Getter 
	@Setter
	private String username;
	
	@Getter 
	@Setter
	private boolean isAccountNonExpired;

	@Getter 
	@Setter
	private boolean isAccountNonLocked;

	@Getter 
	@Setter
	private boolean isCredentialsNonExpired; 

	@Getter 
	@Setter
	private boolean isEnabled;

}