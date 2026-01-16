package com.qa.exceptions;

import java.util.ArrayList;
import java.util.Collection;

public class GYWSecFormatException extends Exception {
	
	private Collection<GYWSecFormatException> problems = new ArrayList<GYWSecFormatException>();

	public GYWSecFormatException() {
		super();
	}

	public GYWSecFormatException(String message, Throwable cause) {
		super(message, cause);
	}

	public GYWSecFormatException(String message) {
		super(message);
	}

	public GYWSecFormatException(Throwable cause) {
		super(cause);
	}

	public Collection<GYWSecFormatException> getProblems() {
		return problems;
	}

	public void addProblem(GYWSecFormatException problem) {
		problems.add(problem);
	}
	
	public int problemCount() {
		return problems.size();
	}


}