package uci.ics.mondego.tldr.tool;

public class StringProcessor {
	
	// convert 'type' of entity to bytecode format to proper format
	// example: "Ljava/util/List;"   --  "java/util/List"
	public static String typeProcessor(String type){
		
		// for one character string it is always primitive type in asm
		if(type.length() == 1)
			return "primitives";
		
	    return type.substring(1, type.length() - 1);
		
	}
	
	public static String pathToFqnConverter(String path){
		return path.replace('/', '.');
	}
	
	private static String convertBaseType(char type) {
	    switch (type) {
	      case 'B':
	        return "byte";
	      case 'C':
	        return "char";
	      case 'D':
	        return "double";
	      case 'F':
	        return "float";
	      case 'I':
	        return "int";
	      case 'J':
	        return "long";
	      case 'S':
	        return "short";
	      case 'Z':
	        return "boolean";
	      case 'V':
	        return "void";
	      default:
	        return "" + type;
	    }
	 }
	
	
}