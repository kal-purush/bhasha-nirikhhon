package uci.ics.mondego.tldr.changeanalyzer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bcel.classfile.ClassParser;
import org.apache.bcel.classfile.Field;
import org.apache.bcel.classfile.JavaClass;
import org.apache.bcel.classfile.Method;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class ClassChangeAnalyzer {
    private static final Logger logger = LogManager.getLogger(ClassChangeAnalyzer.class);
	private final String className;
	private List<String> changedAttributes;
	private Map<String, Long> hashCodes;
	private boolean changed;
	private final ClassParser parser;
	
	public ClassChangeAnalyzer(String className) throws IOException{
		this.className = className;
		this.changedAttributes = new ArrayList<String>();
		this.hashCodes = new HashMap<String, Long>();
		this.changed = false;
		this.parser = new ClassParser(this.className);
		this.parse();
	}
	
	public Long getHashCodeByAttribute(String attr){
		return hashCodes.get(attr);
	}
	
	public boolean hasChanged(){
		return changed;
	}
	
	public List<String> getChangedAttributes(){
		return changedAttributes;
	}
	
	private long fieldHashCode(Field f){
		StringBuilder sb = new StringBuilder();
		sb.append(f.getName());
		sb.append(f.getModifiers());
		sb.append(f.getSignature());
		sb.append(f.getType());
		sb.append(f.getAccessFlags());
		sb.append(f.getConstantValue());
		sb.append(f.getConstantPool().toString());
		sb.append(f.getAttributes().toString());
		return sb.toString().hashCode();
	}
	
	private void parse() throws IOException{
		JavaClass parsedClass = parser.parse();
		
		Field [] allFields = parsedClass.getFields();
		for(Field f: allFields){
			String fieldFqn = parsedClass.getPackageName()+"."+f.getName();
			long currentHashCode = fieldHashCode(f);
			hashCodes.put(fieldFqn, currentHashCode);
			long prevHashCode = -1; /******** GET IT FROM DATABASE *******/
			if(currentHashCode != prevHashCode){
				logger.info(fieldFqn+" changed");
				this.changed = this.changed ? this.changed : true;
				changedAttributes.add(fieldFqn);	
			}
		}
		
		Method [] allMethods= parsedClass.getMethods();
		
		for(Method m: allMethods){
			String methodFqn = parsedClass.getPackageName()+"."+m.getName();
			long currentHashCode = m.getCode().toString().hashCode();
			hashCodes.put(methodFqn, currentHashCode);
			long prevHashCode = -1; /***** GET IT FROM DATABASE *******/
			if(currentHashCode != prevHashCode){
				logger.info(methodFqn+" changed");
				this.changed = this.changed ? this.changed : true;
				changedAttributes.add(methodFqn);
			}
		}
	}

}