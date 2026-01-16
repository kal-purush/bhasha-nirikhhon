
package acme.forms;

import acme.framework.data.AbstractForm;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AuditorDashboard extends AbstractForm {

	// Serialisation identifier -----------------------------------------------

	protected static final long	serialVersionUID	= 1L;

	// Attributes -------------------------------------------------------------

	Integer						theoryAudits;
	Integer						handsOnAudits;
	Double						averageAudtis;
	Double						deviationAudtis;
	Double						minAudits;
	Double						maxAudits;
	Double						averageAuditRecords;
	Double						deviationAuditRecords;
	Double						minAuditRecords;
	Double						maxAuditRecords;

	// Relationships ----------------------------------------------------------

}