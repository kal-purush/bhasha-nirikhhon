package staticTypechecker;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;

public class ModuleHandler {
	private static HashMap<String, Module> modules = new HashMap<>();

	public static void loadModules(String[] moduleNames){
		for(int i = 0; i < moduleNames.length; i++){
			if(!ModuleHandler.modules.containsKey(moduleNames[i])){
				ModuleHandler.modules.put(moduleNames[i], new Module(moduleNames[i]));
			}
		}
	}

	public static void loadModule(String moduleName){
		if(!ModuleHandler.modules.containsKey(moduleName)){
			ModuleHandler.modules.put(moduleName, new Module(moduleName));
		}
	}

	public static Module get(String name){
		return ModuleHandler.modules.get(name);
	}

	public static void addModule(String name){
		ModuleHandler.modules.put(name, new Module(name));
	}

	public static boolean contains(String name){
		return ModuleHandler.modules.containsKey(name);
	}

	public static Collection<Entry<String, Module>> modules(){
		return ModuleHandler.modules.entrySet();
	}
}