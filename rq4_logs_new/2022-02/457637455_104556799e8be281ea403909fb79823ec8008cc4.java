package org.credit.suisse;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.credit.suisse.event.CreditSuisseEvent;
import org.credit.suisse.event.CreditSuisseJSONElement;
import org.credit.suisse.util.CreditSuisseUtil;
import org.credit.suisse.util.HsqldbUtil;
import org.credit.suisse.util.JSONParserUtil;

public class CreditSuisseLogger {

	private static Logger logger = LogManager.getLogger(CreditSuisseLogger.class);
	
	public static void main(String[] args) {
		logger.log(Level.DEBUG, "[CreditSuisseLogger] - start of main() method");
		if(args.length > 0) {
			Map<String, String> argsMap = new LinkedHashMap<>();
		    for (String arg: args) {
		        String[] parts = arg.split("=");
		        argsMap.put(parts[0], parts[1]);
		    }
		    logger.log(Level.DEBUG, "[CreditSuisseLogger] - arguments list passed through command line are: " + argsMap.toString());
		    
		    InputStream input = CreditSuisseUtil.getInputStream(argsMap);
			
			List<CreditSuisseJSONElement> jsonElementList = JSONParserUtil.parseJSON(input);
			
			List<CreditSuisseEvent> creditSuisseEventList = CreditSuisseUtil.generateEventFromJsonElement(jsonElementList);
			
			HsqldbUtil.insertDataList(creditSuisseEventList);
			
		} else {
			logger.log(Level.ERROR, "[CreditSuisseLogger] - please provide the log file path through the command line arguments");
		}
		logger.log(Level.DEBUG, "[CreditSuisseLogger] - end of main() method");
	}
}