/*
 * Copyright (C) 2016 Clayn <clayn_osmato@gmx.de>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.bplaced.clayn.cfs.ext.functional;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * The Functional Wrapper wraps the trying versions of
 * {@link java.util.function functional} interfaces to their original versions.
 * This can be used in places where the original interface is required but you
 * want to use a method that may throw an exception. <br>
 * It's recommended to import the methods of this class directly using
 * {@code import static net.bplaced.clayn.cfs.ext.functional.FunctionalWrapper.*;}
 *
 * @author Clayn <clayn_osmato@gmx.de>
 * @since 0.3.0
 */
public final class FunctionalWrapper
{

    private FunctionalWrapper() throws IllegalAccessException
    {
        throw new IllegalAccessException("No Wrapper for you!");
    }  

    /**
     * Explicit converts the given trying function into a normal one. This can
     * be used when you want to use a method that throws an exception in placed
     * a normal function is required. This method simply returns the given
     * instance.
     *
     * @param <T> the type for the functions argument
     * @param <R> the type of the functions result
     * @param orig the function to convert
     * @return {@code orig}
     * @since 0.3.0
     */
    public static <T, R> Function<T, R> function(TryingFunction<T, R> orig)
    {
        return orig;
    }

    /**
     * Explicit converts the given trying predicate into a normal one. This can
     * be used when you want to use a method that throws an exception in placed
     * a normal predicate is required. This method simply returns the given
     * instance.
     *
     * @param <T> the type for the object to check
     * @param orig the predicate to convert
     * @return {@code orig}
     * @since 0.3.0
     */
    public static <T> Predicate<T> predicate(TryingPredicate<T> orig)
    {
        return orig;
    }

    /**
     * Explicit converts the given trying supplier into a normal one. This can
     * be used when you want to use a method that throws an exception in placed
     * a normal supplier is required. This method simply returns the given
     * instance.
     *
     * @param <T> the type for the suppliers returned object
     * @param orig the predicate to convert
     * @return {@code orig}
     * @since 0.3.0
     */
    public static <T> Supplier<T> supply(TryingSupplier<T> orig)
    {
        return orig;
    }

    /**
     * Explicit converts the given trying unaryoperator into a normal one. This
     * can be used when you want to use a method that throws an exception in
     * placed a normal unaryoperator is required. This method simply returns the
     * given instance.
     *
     * @param <T> the type for the operators argument
     * @param orig the unaryoperator to convert
     * @return {@code orig}
     * @since 0.3.0
     */
    public static <T> UnaryOperator<T> operate(TryingUnaryOperator<T> orig)
    {
        return orig;
    }
}