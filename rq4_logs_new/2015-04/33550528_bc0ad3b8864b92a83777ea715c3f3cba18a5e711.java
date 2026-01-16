package com.sinfonier.bolts;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * PythonBolt Class.
 */
public class LauncherPyBolts {
	
	static final String MarkEmit = "EMIT:";
	
	private String pythonFilePath = "";
	private String moduleProperties = "";
	private String jsonInput = "";
	private Map<String, Object> resultmap = new HashMap<String, Object>();

    public LauncherPyBolts(String xmlFile, String pythonFile) {
        
    	pythonFilePath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath().replaceAll("target.*", "multilang/python/")+pythonFile;
    	moduleProperties = getClass().getClassLoader().getResource("module.properties").getPath();
    	jsonInput = getClass().getClassLoader().getResource("input.json").getPath();
    }

    public Map<String, Object> getJson() {
        return resultmap;
    }
    
    public void run() {
    	
    	ProcessBuilder builder = new ProcessBuilder("python2.7", pythonFilePath, moduleProperties, jsonInput);
    	try {
			Process _subprocess = builder.start();
			BufferedReader reader = new BufferedReader(new InputStreamReader(_subprocess.getInputStream()));
	    	String line = null;
	    	StringBuilder stdout = new StringBuilder();
	    	String emittedjson = "";
	    	while ( (line = reader.readLine()) != null) {
	    		if (line.startsWith(MarkEmit)){
	    			emittedjson = line.replace(MarkEmit, "");
	    		}
	    		else{
	    			stdout.append(line);
	    			stdout.append(System.getProperty("line.separator"));
	    		}
	    		
	    	}
	    	String result = stdout.toString();
	    	System.out.println(result);
	    	
	    	try {
				resultmap = new ObjectMapper().readValue(emittedjson, HashMap.class);
			} catch (Exception e) {
				e.printStackTrace();
			}
	    	
	    	
		} catch (IOException e) {
			e.printStackTrace();
		}

    }

}