package br.com.projects.side.permission;

import javax.persistence.AttributeOverride;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

import br.com.projects.side.utils.BaseEntity;

@Entity
@Table(name = "permissao")
@AttributeOverride(name = "id", column = @Column(name = "idpermissao"))
public class PermissionEntity extends BaseEntity<Long> {
	private static final long serialVersionUID = 1L;

	@Column(name = "tipo", length = 15, nullable = false, unique = true)
	private String role;

	public PermissionEntity() {
	}
	
	public PermissionEntity(String role) {
		super();
		this.role = role;
	}



	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}
	
	
	
	
}