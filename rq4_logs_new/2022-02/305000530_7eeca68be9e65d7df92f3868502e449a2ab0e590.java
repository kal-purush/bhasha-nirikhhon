/*
 * Copyright 2020 Miroslav Pokorny (github.com/mP1)
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

package walkingkooka.tree.expression.function.number;

import walkingkooka.Cast;
import walkingkooka.collect.list.Lists;
import walkingkooka.text.CharSequences;
import walkingkooka.tree.expression.ExpressionNumber;
import walkingkooka.tree.expression.function.ExpressionFunctionContext;
import walkingkooka.tree.expression.function.ExpressionFunctionParameter;
import walkingkooka.tree.expression.function.ExpressionFunctionParameterName;

import java.util.List;
import java.util.Optional;

// https://support.google.com/docs/answer/9084167?hl=en&ref_topic=3105474
final class StringExpressionFunctionBase<C extends ExpressionFunctionContext> extends StringExpressionFunction<C> {

    /**
     * Instance getter.
     */
    static <C extends ExpressionFunctionContext> StringExpressionFunctionBase<C> instance() {
        return Cast.to(INSTANCE);
    }

    /**
     * Singleton
     */
    private static final StringExpressionFunctionBase<?> INSTANCE = new StringExpressionFunctionBase<>();

    /**
     * Private ctor
     */
    private StringExpressionFunctionBase() {
        super("base");
    }

    @Override
    public List<ExpressionFunctionParameter<?>> parameters() {
        return PARAMETERS;
    }

    private final static ExpressionFunctionParameter<ExpressionNumber> NUMBER = ExpressionFunctionParameter.NUMBER;

    private final static ExpressionFunctionParameter<ExpressionNumber> BASE = ExpressionFunctionParameterName.with("base")
            .required(ExpressionNumber.class);

    private final static ExpressionFunctionParameter<ExpressionNumber> MIN_LENGTH = ExpressionFunctionParameterName.with("min-length")
            .optional(ExpressionNumber.class);

    private final static List<ExpressionFunctionParameter<?>> PARAMETERS = Lists.of(
            NUMBER,
            BASE,
            MIN_LENGTH
    );

    @Override
    public String apply(final List<Object> parameters,
                        final C context) {
        this.checkParameterCount(parameters);

        final ExpressionNumber number = NUMBER.getOrFail(parameters, 0);

        final ExpressionNumber base = BASE.getOrFail(parameters, 1);

        final Optional<ExpressionNumber> minLength = MIN_LENGTH.get(parameters, 2);

        final String string = number.toStringWithBase(base)
                .toUpperCase(context.locale());

        return minLength.isPresent() ?
                padLeftZeroIfNecessary(string, minLength.get().intValueExact()) :
                string;
    }

    private static String padLeftZeroIfNecessary(final String string,
                                                 final int minLength) {
        return string.length() > minLength ?
                string :
                CharSequences.padLeft(string, minLength, '0').toString();
    }
}