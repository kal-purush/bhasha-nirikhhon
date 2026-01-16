package com.app.colibri.service;

import java.util.Locale;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.stereotype.Component;

import com.app.colibri.controller.WordController;

import lombok.Data;

@Component
@Data
public class AppSettings {

	public static AppSettings appSettings;

	@Autowired
	private MessageSource localeSource;
	private Locale appLocale = Locale.forLanguageTag("EN");

	public AppSettings() {
	}

	public String obtainLocaleSourceItem(String code) {
		try {
			return localeSource.getMessage(code, null, appLocale);
		} catch (Exception e) {
			return "";
		}
	}

	public void installLocale(String locale) {
		try {
			appLocale = Locale.forLanguageTag(locale);
		} catch (Exception e) {
		}
	}

	public static String getLocaledItem(String code) {
		return appSettings.obtainLocaleSourceItem(code);
	}

	public static void changeLocale(String locale) {
		appSettings.installLocale(locale);
		WordController.userDataRegistry.setAppLocale(locale);
		MainLocaleManager.changeLocaleStatic();
	}

}