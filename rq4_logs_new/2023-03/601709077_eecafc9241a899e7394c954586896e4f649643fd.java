
package acme.roles;

import java.util.Collection;

import javax.persistence.Entity;
import javax.persistence.OneToMany;
import javax.validation.constraints.NotBlank;

import org.hibernate.validator.constraints.Length;
import org.hibernate.validator.constraints.URL;

import acme.entitites.tutorial.Tutorial;
import acme.framework.data.AbstractRole;
import lombok.Getter;
import lombok.Setter;

@Entity
@Getter
@Setter
public class Assistant extends AbstractRole {

	protected static final long		serialVersionUID	= 1L;

	@NotBlank
	@Length(max = 75)
	protected String				supervisor;

	@NotBlank
	@Length(max = 100)
	protected String				curriculum;

	@URL
	protected String				link;

	//Relations -----------------------------------------------------------------

	@OneToMany(mappedBy = "assistantTutorial")
	protected Collection<Tutorial>	tutorials;

}