package ru.itmo.compiler.codegen.jvm;

import java.util.Arrays;

public class JVMBytecodeDirective extends JVMBytecodeEntity {
	private String directiveName;
	private String[] args;
	
	public JVMBytecodeDirective(String directiveName, Object... args) {
		this.directiveName = directiveName;
		this.args = Arrays.stream(args).map(Object::toString).toArray(String[]::new);
	}
	
	@Override
	public String toString() {
		return String.format(
					".%s %s",
					directiveName,
					String.join(" ", args)
				);
	}
}