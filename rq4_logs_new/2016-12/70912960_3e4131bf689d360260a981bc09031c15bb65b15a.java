/*
 * Copyright(c) 2016 - Heliosphere Corp.
 * ---------------------------------------------------------------------------
 * This file is part of the Heliosphere's project which is licensed under the 
 * Apache license version 2 and use is subject to license terms.
 * You should have received a copy of the license with the project's artifact
 * binaries and/or sources.
 * 
 * License can be consulted at http://www.apache.org/licenses/LICENSE-2.0
 * ---------------------------------------------------------------------------
 */
package com.heliosphere.demeter.base.resource;

/**
 * Exception thrown to indicate an error occurred while processing a resource.
 * <hr>
 * @author <a href="mailto:christophe.resse@gmail.com">Christophe Resse - Heliosphere</a>
 * @version 1.0.0
 */
public class ResourceException extends Exception
{
	/**
	 * Serialization identifier.
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Exception thrown to indicate an error occurred while processing a resource.
	 */
	public ResourceException()
	{
		super();
	}

	/**
	 * Exception thrown to indicate an error occurred while processing a resource.
	 * <p>
	 * @param exception Parent exception.
	 */
	public ResourceException(final Exception exception)
	{
		super(exception);
	}

	/**
	 * Exception thrown to indicate an error occurred while processing a resource.
	 * <p>
	 * @param message Message describing the error being the cause of the raised exception.
	 */
	public ResourceException(final String message)
	{
		super(message);
	}

	/**
	 * Exception thrown to indicate an error occurred while processing a resource.
	 * <p>
	 * @param message Message describing the error being the cause of the raised exception.
	 * @param exception Parent exception.
	 */
	public ResourceException(final String message, final Exception exception)
	{
		super(message, exception);
	}
}