/*
 * Copyright(c) 2016 Heliosphere Corp.
 * ---------------------------------------------------------------------------
 * This file is part of the Heliosphere's project which is licensed under the
 * Apache license version 2 and use is subject to license terms. You should have
 * received a copy of the license with the project's artifact binaries and/or
 * sources.
 *
 * License can be consulted at http://www.apache.org/licenses/LICENSE-2.0
 * ---------------------------------------------------------------------------
 */
package com.heliosphere.demeter.base.exception;

/**
 * Interface to be implemented by all exception enumeration classes.
 * <p>
 * This interface allows to create exceptions based on an enumerated value such as in the following example:
 * <p>
 * <code>
 * 		throw new ApplicationException(ApplicationExceptionType.AlreadyRunning);
 * 	</code>
 * <p>
 * The use of this syntax requires that the {@code ApplicationExceptionType} enumeration class implements the {@code IExceptionEnum} interface.
 * <hr>
 * @author <a href="mailto:christophe.resse@gmail.com">Christophe Resse - Heliosphere
 * @version 1.0.0
 */
public interface IExceptionType
{
	/**
	 * Returns the message associated to this exception.
	 * <hr>
	 * @return Message.
	 */
	public String getMessage();
}