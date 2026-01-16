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
package com.heliosphere.demeter.base.exception;

import java.text.MessageFormat;

import com.heliosphere.demeter.base.resource.bundle.IBundle;
import com.heliosphere.demeter.base.resource.bundle.ResourceBundleManager;

/**
 * An abstract implementation of a checked exception.
 * <p>
 * A checked exception represents invalid conditions in areas outside the immediate control 
 * of the program (invalid user input, database problems, network outages, absent files) are 
 * subclasses of {@link Exception}.
 * <p>
 * A method is obliged to establish a policy for all checked exceptions thrown by its 
 * implementation (either pass the checked exception further up the stack, or handle it somehow).
 * <p>
 * It is somewhat confusing, but note as well that {@link RuntimeException} (unchecked) is itself 
 * a subclass of {@link Exception} (checked).
 * <hr>
 * @author <a href="mailto:christophe.resse@gmail.com">Christophe Resse - Heliosphere</a>
 * @version 1.0.0
 */
public abstract class AbstractCheckedException extends Exception
{
	/**
	 * Default serialization identifier.
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Exception key.
	 */
	protected Enum<? extends IExceptionType> key;

	/**
	 * Creates a new checked exception.
	 */
	public AbstractCheckedException()
	{
		super();
	}

	/**
	 * Creates a new checked exception based on an exception key.
	 * <hr>
	 * @param key Resource bundle key (enumerated value coming from an enumeration implementing the {@link IBundle} interface).
	 */
	public AbstractCheckedException(final Enum<? extends IBundle> key)
	{
		super(ResourceBundleManager.getMessage(key));
	}

	/**
	 * Creates a new checked exception based on a parent exception.
	 * <hr>
	 * @param exception Parent exception.
	 */
	public AbstractCheckedException(final Exception exception)
	{
		super(exception);
	}

	/**
	 * Creates a new checked exception based on a message.
	 * <hr>
	 * @param message Message of the exception.
	 */
	public AbstractCheckedException(final String message)
	{
		super(message);
	}

	/**
	 * Creates a new checked exception based on a message and a parent exception.
	 * <hr>
	 * @param message Message of the exception.
	 * @param exception Parent exception.
	 */
	public AbstractCheckedException(final String message, final Exception exception)
	{
		super(message + exception.getMessage(), exception);
	}

	/**
	 * Creates a new checked exception based on an exception enumerated value and given parameters.
	 * <hr>
	 * @param key Exception key (enumerated value coming from an enumeration implementing the {@link IExceptionType} interface).
	 * @param parameters List of parameters used to populate the exception message.
	 */
	@SuppressWarnings({ "unchecked" })
	public AbstractCheckedException(final Enum<?> key, final Object... parameters)
	{
		super(key instanceof IBundle ? MessageFormat.format(ResourceBundleManager.getMessage((Enum<? extends IBundle>) key), parameters) : MessageFormat.format(((IExceptionType) key).getMessage(), parameters));

		// Do we have an exception in the parameter list?
		for (Object o : parameters)
		{
			if (o instanceof AbstractCheckedException) // Do not override the original error code!
			{
				this.key = ((AbstractCheckedException) o).getKey();
				return;
			}
		}

		if (key instanceof IExceptionType)
		{
			this.key = (Enum<? extends IExceptionType>) key;
		}
	}

	/**
	 * Returns the exception key.
	 * <hr>
	 * @return Exception key.
	 */
	public final Enum<? extends IExceptionType> getKey()
	{
		return key;
	}

	/**
	 * Returns the exception error class.
	 * <hr>
	 * @return Exception error class.
	 */
	public final Class<? extends IExceptionType> getErrorClass()
	{
		return key.getDeclaringClass();
	}

	@Override
	protected final Object clone() throws CloneNotSupportedException
	{
		return new CloneNotSupportedException();
	}
}