/*
 * Copyright 2019 Miroslav Pokorny (github.com/mP1)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package walkingkooka.convert;

import walkingkooka.Either;
import walkingkooka.reflect.StaticHelper;
import walkingkooka.text.CharSequences;

/**
 * Default methods shared by both {@link Converter} and {@link ConverterContext}.
 */
final class FailConversion implements StaticHelper {

    /**
     * Useful to report a failed conversion with a standard error message.
     */
    static <T> Either<T, String> handle(final Object value,
                                        final Class<T> target) {
        return handle0(value, target, null);
    }

    /**
     * Useful to report a failed conversion with a standard error message, which includes a {@link Throwable#getMessage()}.
     */
    static <T> Either<T, String> handle(final Object value,
                                        final Class<T> target,
                                        final Throwable cause) {
        final String message = cause.getMessage();
        return handle0(value, target, (CharSequences.isNullOrEmpty(message) ? cause.getClass().getName() : cause.getMessage()));
    }

    private static <T> Either<T, String> handle0(final Object value,
                                                 final Class<T> target,
                                                 final String message) {
        return Either.right("Failed to convert " + CharSequences.quoteIfChars(value) + " (" + value.getClass().getName() + ") to " + target.getName() +
                (CharSequences.isNullOrEmpty(message) ? "" : ", " + message));
    }

    /**
     * Stop creation
     */
    private FailConversion() {
        throw new UnsupportedOperationException();
    }
}