const fs = require('fs');

const sqlFormat = "CREATE TABLE IF NOT EXISTS LAST_READER (ID SERIAL PRIMARY KEY, VALUE JSON NOT NULL);\n" +
  "\n" +
  "DROP TABLE IF EXISTS TRANSLATIONS;\n" +
  "CREATE TABLE TRANSLATIONS (ID SERIAL PRIMARY KEY, VALUE JSON NOT NULL);\n" +
  "{0}\n" +
  "\n";

// Read in the translations
const translationFiles = ['english_translations.json', 'german_translations.json'];

var insertTranslationStatements = '';
for(var i = 0; i < translationFiles.length; ++i) {
  const translations = JSON.parse(fs.readFileSync('data/' + translationFiles[i]));
  const insertStmt = 'INSERT INTO TRANSLATIONS (VALUE) VALUES (\'' + JSON.stringify(translations) + '\');';
  insertTranslationStatements = insertTranslationStatements + insertStmt + "\n";
}

const sql = sqlFormat.replace("{0}", insertTranslationStatements);

fs.writeFileSync('scripts/create_database.sql', sql);