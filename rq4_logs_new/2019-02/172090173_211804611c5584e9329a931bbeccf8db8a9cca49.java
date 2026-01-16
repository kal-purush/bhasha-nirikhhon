package com.pimpelkram.inventory.server.config;

import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Baut aus einer JSON-Datei das Settings-Objekt. Alle JsonStrings werden ohne
 * umschließende Anführungszeichen übernommen.
 * @author borsutzha */
public class JsonSettingsBuilder {

    Logger logger = LoggerFactory.getLogger(JsonSettingsBuilder.class);

    public Settings build(Settings settings, JsonReader reader) {
        if (settings == null) {
            settings = new Settings();
        }
        try {
            final JsonObject jobject = reader.readObject();
            // Property 1: Wurzelverzeichnis(se) für das Einlesen von
            // Lieferungen:
            final JsonString jsonServerFolder = jobject.getJsonString("serverFolder");
            settings.setServerFolder(jsonServerFolder.getString());
        } catch (final Exception e) {
            this.logger.error("Fehler beim Auslesen der JSON Konfigurationsdatei", e);
        }
        return settings;
    }
}