package com.google.codeu.utils;

import com.google.cloud.translate.Translate;
import com.google.cloud.translate.Translate.TranslateOption;
import com.google.cloud.translate.TranslateOptions;
import com.google.cloud.translate.Translation;

public class Translator {
  public static String translate(String originalText, String targetLanguage) throws Exception {

    Translate translate = TranslateOptions.getDefaultInstance().getService();
	Translation translation =
        translate.translate(originalText, TranslateOption.targetLanguage(targetLanguage));

    return translation.getTranslatedText();
  }
}