package de.gaidos.simplerestinterface.exceptionhandling;

public class ServerException extends RuntimeException {

	private static final long serialVersionUID = -859585882744992623L;

	public ServerException(String exception) {
	    super(exception);
	  }

	}