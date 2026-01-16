package filter;

import java.util.Iterator;
import java.util.Vector;
import org.json.JSONArray;
import org.json.JSONObject;

import stats.Stats;
import stats.StatsManagement;
import exception.VoidGetException;
import exception.WrongCountryException;

/** Questa classe contiene i metodi necessari al filtraggio degli eventi per le rotte /total e /stats
 *  nel caso in cui l'utente ha selezionato tutti i generi.
 * 
 * @author Domenico Ciavarella
 * @author Daniele Cota
 *
 */


public class CountryFilter  {
	
	Stats stats = new StatsManagement();
	
	VoidGetException Exception = new VoidGetException();
	
	
	 /** Questo metodo calcola il numero totale di eventi che hanno luogo nei paesi inseriti.
	 * Restituisce un JSONArray contenente JSONObject che rappresentano i paesi
	 * e il numero totale di eventi di ciascuno di essi.
	 * 
	 * @param countries 
	 * @throws WrongCountryException se viene inserito il countryCode di un paese non europeo. 
	 */
	
	public JSONArray TotalEventsCountry(Vector<String> countries) throws WrongCountryException {
		
	Exception.countryVectorException(countries);
		
		JSONArray countryEvents = new JSONArray();
		
		Iterator<String> iterator = countries.iterator();
		
		int i = 0;

	
		while (iterator.hasNext()) {
		
			
		JSONObject object = new JSONObject();
		object = stats.getCountryEvents(iterator.next());
		int totalElements = object.getInt("totalEvents");
		
		JSONObject total = new JSONObject();
		total.put("country", countries.get(i));
		total.put("totalEvents", totalElements);
		
		countryEvents.put(total);
		
		i++;
		
		}
	
	return countryEvents;
	}


	
	 /** Questo metodo calcola la media, il massimo e il minimo del numero di eventi che hanno luogo nei paesi scelti dall'utente.
	 * Restituisce un JSONArray contenente JSONObject che mostra qual è il numero medio di eventi dei paesi inseriti
	 * e quale di essi ospita il maggior o il minor numero di eventi.
	 * 
	 * @param countries 
	 * @throws WrongCountryException se viene inserito il countryCode di un paese non europeo. 
	 */
	
public JSONArray StatsEventsCountry(Vector<String> countries) throws WrongCountryException {
		
	Exception.countryVectorException(countries);
		
		JSONArray countryEvents = new JSONArray();
		
		Iterator<String> iterator = countries.iterator();
		
		int i = 0;
		int TotalCountriesEvents = 0;
		
		String maxCountry = null;
		String minCountry = null;
		int maxEvent = 0;
		int minEvent = 999999;
	
		while (iterator.hasNext()) {
		
			
		JSONObject object = new JSONObject();
		object = stats.getCountryStats(iterator.next());
		int totalElements = object.getInt("totalEvents");
		
		
		
		TotalCountriesEvents += totalElements;
		
		
		if(totalElements <= minEvent) {
			minEvent=totalElements;
			minCountry = countries.get(i);
		}
		
		if (totalElements >= maxEvent) {
			maxEvent = totalElements;
			maxCountry = countries.get(i);
		}
		i++;
		
		}

		double average = 0;
		
		double TotalEvents = TotalCountriesEvents;
		
		double j = i;
		
		average = TotalEvents/j;
		
		double averageRound= Math.round(average * 100.0) / 100.0;
		
		JSONObject averageCountryEvents = new JSONObject();
		averageCountryEvents.put("Average Country Events", averageRound);
		
		JSONObject maxCountryEvents = new JSONObject();
		maxCountryEvents.put("Country with most events", maxCountry);
		maxCountryEvents.put("totalEvents: ", maxEvent);
		
		
		JSONObject minCountryEvents = new JSONObject();
		minCountryEvents.put("Country with least events", minCountry);
		minCountryEvents.put("totalEvents: ", minEvent);
		
		countryEvents.put(averageCountryEvents);
		countryEvents.put(maxCountryEvents);
		countryEvents.put(minCountryEvents);
	
	return countryEvents;
	}
}