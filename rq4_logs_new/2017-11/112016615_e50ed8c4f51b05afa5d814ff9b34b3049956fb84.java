/*
 * Copyright 2017 afarre.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bytemechanics.typeex;

import org.bytemechanics.typeex.impl.TypifiedException;
import org.bytemechanics.typeex.internal.SimpleFormat;
import org.bytemechanics.typeex.internal.TypeExHelper;
import java.util.Arrays;
import java.util.function.Supplier;

/**
 * Main interface to apply over any exception type. Can be applied also over standard object or over enum although recommended place is over an enum. This interface also extend a supplier of
 * TypifiableException
 *
 * @param <T> Something extending typifiable exception
 * @see TypifiableException
 * @see Supplier
 * @author e103880
 * @since 0.1.0
 */
public interface ExceptionType<T extends TypifiableException> extends Supplier<T> {

	/**
	 * Name of the exception type Note: It's mandatory to implement this method if the implementing class is not an enum, otherwise the enum already implements name() method
	 *
	 * @return the name (never should be null)
	 * @since 0.1.0
	 */
	public String name();

	/**
	 * Message to use with the exception generated Note: It's mandatory to implement this method The message will be formatted with the arguments passed replacing the expression {} with the argument
	 * corresponding with the {} number of occurrence. 
	 * <br>Example: "This {} is an exception: {}" with arguments: ("A","B") will be converted to "This A is an exception: B"
	 *
	 * @return the exception message for this kind of exception
	 * @since 0.1.0
	 */
	public String getMessage();

	/**
	 * Exception class to use with this kind of exception 
	 * <br>Note: It's not mandatory to implement if TypifiedException is the type of exception you want to raise
	 *
	 * @return The T class assigned to this exception (by default TypifiedException)
	 * @see TypifiedException
	 * @see TypifiableException
	 * @since 0.3.0
	 */
	public default Class<? extends T> getExceptionClass() {
		return (Class<T>)TypifiedException.class;
	}

	/**
	 * Returns the instance of the getExceptionClass() with this typified exception
	 *
	 * @return The T instance
	 * @see TypifiableException
	 * @since 0.3.0
	 */
	@Override
	public default T get() {
		return TypeExHelper.findSuitableConstructor(this)
							.flatMap(constructor -> TypeExHelper.instance(constructor,null, this))
							.map(instance -> (T)instance)
							.orElseThrow(() -> new Error(SimpleFormat.format("Unable to find any suitable constructor for class {} with arguments {}"
																	,this.getExceptionClass(),Arrays.asList(new Object[]{Throwable.class,this.getClass(),Object[].class}))));
	}

	/**
	 * Returns the instance of the getExceptionClass() with this typified exception with the given cause
	 *
	 * @param _cause cause of the exception
	 * @return The T instance
	 * @see TypifiableException
	 * @since 0.3.0
	 */
	public default T from(final Throwable _cause) {
		return TypeExHelper.findSuitableConstructor(this)
							.flatMap(constructor -> TypeExHelper.instance(constructor,_cause, this))
							.map(instance -> (T)instance)
							.orElseThrow(() -> new Error(SimpleFormat.format("Unable to find any suitable constructor for class {} with arguments {}"
																	,this.getExceptionClass(),Arrays.asList(new Object[]{Throwable.class,this.getClass(),Object[].class}))));
	}

	/**
	 * Returns the instance of the getExceptionClass() with this typified exception with the given arguments
	 *
	 * @param _args arguments to replace to the getMessage() text with the same format basis explained above
	 * @return The T instance
	 * @see TypifiableException
	 * @since 0.3.0
	 */
	public default T with(final Object... _args) {
//System.out.println("ARGS:" +Arrays.asList(_args));
		return TypeExHelper.findSuitableConstructor(this)
							.flatMap(constructor -> TypeExHelper.instance(constructor,null, this,_args))
							.map(instance -> (T)instance)
							.orElseThrow(() -> new Error(SimpleFormat.format("Unable to find any suitable constructor for class {} with arguments {}"
																	,this.getExceptionClass(),Arrays.asList(new Object[]{Throwable.class,this.getClass(),Object[].class}))));
	}
}