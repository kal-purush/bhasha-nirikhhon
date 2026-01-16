
package acme.components;

import java.text.SimpleDateFormat;
import java.util.Date;

import acme.framework.components.datatypes.Money;

public class InternationalizeFormatter {

	public String booleanParse(final boolean b, final String language) {
		switch (language) {
		case "en":
			return b ? "Yes" : "No";
		case "es":
			return b ? "Sí" : "No";
		default:
			return "Error";
		}
	}

	public String dateParse(final Date d, final String language) {
		switch (language) {
		case "en":
			final SimpleDateFormat dateFormatEn = new SimpleDateFormat("yyyy/dd/MM hh:mm");
			return dateFormatEn.format(d);
		case "es":
			final SimpleDateFormat dateFormatEs = new SimpleDateFormat("yyyy/MM/dd hh:mm");
			return dateFormatEs.format(d);
		default:
			return "Error";
		}
	}

	public String moneyParse(final Money m, final String language) {
		switch (language) {
		case "en":
			return String.format("%s %s", m.getAmount(), m.getCurrency()).replace(",", ".");
		case "es":
			return String.format("%s %s", m.getAmount(), m.getCurrency());
		default:
			return "Error";
		}
	}

}