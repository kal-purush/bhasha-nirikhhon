package com.guideme.pattern.buider;

import java.util.ArrayList;
import java.util.List;

public class BuilderExcercise {

	public static void main(String[] args) {
		CodeBuilder cb = new CodeBuilder("Person").addField("firstName", "String").addField("lastName", "String").addField("Phone #", "int");
		System.out.println(cb);
	}

}

class Field {
	private String name, type;

	public Field(String name, String type) {
		this.name = name;
		this.type = type;
	}

	@Override
	public String toString() {
		return "Field [name=" + name + ", type=" + type + "]";
	}
}

class ClassBuilder {
	String name;
	List<Field> fields = new ArrayList<>();

	public ClassBuilder() {
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		String nl = System.lineSeparator();
		sb.append("public class " + name).append(nl).append("{").append(nl);
		for (Field f : fields)
			sb.append("  " + f).append(nl);
		sb.append("}").append(nl);
		return sb.toString();
	}
}

class CodeBuilder{
	private ClassBuilder theClass = new ClassBuilder();

	public CodeBuilder(String rootName) {
		theClass.name = rootName;
	}
	
	public CodeBuilder addField(String name,String type) {
		theClass.fields.add(new Field(name, type));
		return this;
	}

	@Override
	public String toString() {
		return "CodeBuilder [theClass=" + theClass.toString() + "]";
	}
	
	
	
}