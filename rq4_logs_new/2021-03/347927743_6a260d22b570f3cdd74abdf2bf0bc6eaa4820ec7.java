package it.cnr.asfa.textprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.LinkedHashMap;

import org.json.JSONObject;

import it.cnr.asfa.textprocessing.utils.EfficientSearchInText;

public class ASFAResearchObject {

	// Testo pulito estratto dal file JSON
	String puretext;

	// Lista di corrispondenze riscontrate nel testo espresse in booleani
	boolean[] matches;
	
	// Stringa che descrive la tipologia della annotazioni richieste, nel caso specifico Taxon ma che potrebbe diventare Worms o Gibf
	String category;

	// le dichiarazioni precedenti "LinkedHashMap<String,String> annotationstext;"
	// causavano l'errore "Exception in thread "main"
	// java.lang.NullPointerException "
	LinkedHashMap<String, String> annotationstext = new LinkedHashMap<String, String>();
	LinkedHashMap<String, String> jsonAnnotations = new LinkedHashMap<String, String>();

	// Metodo che ottiene il testo pulito dal file JSON ricavato dall'output di
	// NLPHub
	public void capture(String filename) throws IOException {
		FileReader file = new FileReader(filename);
		BufferedReader reader = new BufferedReader(file);

		String key = "";
		String line = reader.readLine();

		while (line != null) {
			key += line;
			line = reader.readLine();
		}

		String jsonString = key;
		JSONObject obj = new JSONObject(jsonString);
		puretext = obj.getString("text");
	}

	// Metodo identifica le tassonomie e nel testo e le salva nella
	// variabile "matches"
	public void get() throws Exception {

		String[] TokenizedjsonText = puretext.split("\\s+");
		// Per migliorare il match rimuovo i segni di punteggiatura e trasformo il testo
		// in minuscolo. Questo passaggio viene effettuato dopo la suddivisione del
		// testo in tokens per eviare che si verifichino delle incogruenze a livello di
		// match di tokens nel metodo "enrich" dove il testo deve essere quello
		// originale
		// dell'output del json e non quello trasformato per effettuare al meglio il
		// match
		for (int i = 0; i < TokenizedjsonText.length; i++) {
			TokenizedjsonText[i] = TokenizedjsonText[i].replaceAll("[^a-zA-Z ]", "").toLowerCase();
		}
		EfficientSearchInText est = new EfficientSearchInText();
		File referenceTaxa = new File("taxon.csv");
		int nthreads = 8;

		boolean found[] = est.searchParallel(TokenizedjsonText, referenceTaxa, nthreads);
		System.out.println("Found " + Arrays.toString(found));
		matches = found;
	}

	public void enrich(String annotationName) throws IOException {

		// Salvo la stringa che indica la categoria delle annotazioni in una variabile che utilizzerò successiamente
		category = annotationName;
		String[] TokenizedjsonText = puretext.split("\\s+");
		// Variabile per il file TXT
		StringBuffer sb = new StringBuffer();
		// Variabile per il file JSON
		StringBuffer annotation = new StringBuffer();
		// Coppia di variabili per il conteggio degli indici delle posizioni di inizio e
		// fine delle parole nel testo (quelli che servono per il file JSON)
		int s0 = 0;
		int s1 = 0;
		annotation.append("\"" + annotationName + "\":");
		for (int i = 0; i < TokenizedjsonText.length; i++) {
			if (matches[i] == true) {
				sb.append(" [" + TokenizedjsonText[i] + "]");
				s1 = s1 + TokenizedjsonText[i].length();
				annotation.append("{\"indices\":[" + s0 + "," + s1 + "]}");
				s0 = s0 + TokenizedjsonText[i].length() + 1;
				s1 = s1 + 1;
			} else
				sb.append(" " + TokenizedjsonText[i]);
			s1 = s1 + TokenizedjsonText[i].length() + 1;
			s0 = s0 + TokenizedjsonText[i].length() + 1;

		}

		String speciesAnnotation = sb.toString();
		System.out.println(category + speciesAnnotation);
		annotationstext.put(annotationName, speciesAnnotation);
		jsonAnnotations.put(annotationName, annotation.toString());

	}

	// Rimuovo le annotazioni superflue (tutte tranne quella di merge dal file .JSON
	// e aggiung quelle di Taxon)
	public void materializeText(String filename) throws IOException {
		File file = new File(filename);
		File temp = File.createTempFile("file", ".txt", file.getParentFile());
		String charset = "UTF-8";
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
		PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(temp), charset));
		int count = 0;
		for (String line; (line = reader.readLine()) != null;) {
			if (line.contains("##") && count != 1) {
				break;
			}
			count = count + 1;
			writer.println(line);
		}

		// Stampo le nuove annotazioni successivamente a quelle di MERGE e chiudo
		writer.append("##" + category.toUpperCase() + "##" );
		writer.print(annotationstext.get(category));
		reader.close();
		writer.close();
		file.delete();
		temp.renameTo(file);
	}

	// Rimuovo le annotazioni superflue (tutte tranne quella di merge dal file .JSON
	// e aggiung quelle di Taxon)
	public void materializeJSON(String filename) throws IOException {

		FileReader file = new FileReader(filename);
		BufferedReader reader = new BufferedReader(file);

		String key = "";
		String line = reader.readLine();

		while (line != null) {
			key += line;
			line = reader.readLine();
		}

		String jsonString = key;
		JSONObject obj = new JSONObject(jsonString);
		// Questo elemento mancante dal file json non causa errore
		obj.remove("prova");
		obj.remove(
				"org.gcube.dataanalysis.wps.statisticalmanager.synchserver.mappedclasses.transducerers.ENGLISH_NER_CORENLP");
		obj.remove(
				"org.gcube.dataanalysis.wps.statisticalmanager.synchserver.mappedclasses.transducerers.OPEN_NLP_ENGLISH_PIPELINE");
		obj.remove(
				"org.gcube.dataanalysis.wps.statisticalmanager.synchserver.mappedclasses.transducerers.TAGME_ENGLISH_NER");
		obj.remove(
				"org.gcube.dataanalysis.wps.statisticalmanager.synchserver.mappedclasses.transducerers.ANNIE_PLUS_MEASUREMENTS");
		obj.remove(
				"org.gcube.dataanalysis.wps.statisticalmanager.synchserver.mappedclasses.transducerers.ENGLISH_NAMED_ENTITY_RECOGNIZER");
		obj.remove(
				"org.gcube.dataanalysis.wps.statisticalmanager.synchserver.mappedclasses.transducerers.KEYWORDS_NER_ENGLISH");
		BufferedWriter writer = new BufferedWriter(new FileWriter(filename, false));
		writer.append("");
		//Salvo il JSON rimuovando l'ultimo carattere che è una parentesi
		writer.append(obj.toString().substring(0, obj.toString().length() - 1));
		// Aggiungo quelle di Taxon
		writer.append("," + jsonAnnotations.get(category).toString());
		writer.close();
	}

}