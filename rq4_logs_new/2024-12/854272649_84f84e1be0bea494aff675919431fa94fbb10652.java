package com.rumisystem.rumi_smtp.MODULE;

import static com.rumisystem.rumi_smtp.Main.CONFIG_DATA;

public class ADDRESS_DOMAIN {
	public static boolean Check(String CHECK_DOMAIN) {
		for (String DOMAIN:CONFIG_DATA.get("SMTP").asString("DOMAIN").split(",")) {
			if (CHECK_DOMAIN.equals(DOMAIN)) {
				return true;
			}
		}

		return false;
	}
}