/*
 * Dashboard.java
 *
 * Copyright (C) 2012-2023 Rafael Corchuelo.
 *
 * In keeping with the traditional purpose of furthering education and research, it is
 * the policy of the copyright owner to permit non-commercial use and redistribution of
 * this software. It has been tested carefully, but it is not guaranteed for any particular
 * purposes. The copyright owner does not offer any warranties or representations, nor do
 * they accept any liabilities with respect to them.
 */

package acme.forms;

import java.util.Map;

import acme.framework.data.AbstractForm;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class AdminDashboard extends AbstractForm {

	// Serialisation identifier -----------------------------------------------

	protected static final long	serialVersionUID	= 1L;

	// Attributes -------------------------------------------------------------

	Integer						numberOfAdministrator;
	Integer						numberOfStudent;
	Integer						numberOfAuditor;
	Integer						numberOfAssistant;

	Double						ratioPeepsWithEmailAndLink;
	Double						ratioOfNonCriticalBulletins;

	Map<String, Double>			averageBudgetInOffersByCurrency;
	Map<String, Double>			deviationBudgetInOffersByCurrency;
	Map<String, Double>			minBudgetInOffersByCurrency;
	Map<String, Double>			maxBudgetInOffersByCurrency;

	Double						averageNumberOfNotesInLastTenWeeks;
	Double						deviationNumberOfNotesInLastTenWeeks;
	Double						minNumberOfNotesInLastTenWeeks;
	Double						maxNumberOfNotesInLastTenWeeks;

	// Derived attributes -----------------------------------------------------

	// Relationships ----------------------------------------------------------

}