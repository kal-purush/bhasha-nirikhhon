package ru.itmo.compiler.codegen.jvm;

import java.util.Arrays;
import java.util.List;

import ru.itmo.compiler.codegen.jvm.utils.JVMBytecodeUtils;

public class JVMBytecodeClass extends JVMBytecodeEntity {
	private AccessSpec[] accessSpecs;
	private String className;
	private List<JVMBytecodeField> fields;
	private List<JVMBytecodeMethod> methods;
	
	public JVMBytecodeClass(AccessSpec[] accessSpecs, String className, List<JVMBytecodeField> fields, List<JVMBytecodeMethod> methods) {
		this.accessSpecs = accessSpecs;
		this.className = JVMBytecodeUtils.formatDescriptor(className);
		
		this.fields = fields;
		this.methods = methods;
	}
	
	public AccessSpec[] getAccessSpecs() {
		return accessSpecs;
	}
	
	public String getClassName() {
		return className;
	}
	
	public List<JVMBytecodeField> getFields() {
		return fields;
	}
	
	public List<JVMBytecodeMethod> getMethods() {
		return methods;
	}

	@Override
	public String toString() {
		return String.format(
				".class %s %s\n"
				+ ".super java/lang/Object\n"
				+ "\n%s\n"
				+ "\n%s",
				
				String.join(
					" ", 
					Arrays
						.stream(accessSpecs)
						.map(spec -> spec.name().toLowerCase())
						.toList()
				), 
				className,
				
				String.join(
					"\n", 
					fields
						.stream()
						.map(JVMBytecodeField::toString)
						.toList()
				),
				
				String.join(
					"\n\n", 
					methods
						.stream()
						.map(JVMBytecodeMethod::toString)
						.toList()
				)
			);
	}
	
	public static enum AccessSpec {
		PUBLIC,
		FINAL,
		SUPER,
		ABSTRACT,
		INTERFACE,
	}
}