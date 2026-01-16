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
import walkingkooka.tree.expression.ExpressionNumber;
import walkingkooka.tree.expression.ExpressionNumberKind;
import walkingkooka.tree.expression.function.ExpressionFunctionContext;
import walkingkooka.tree.expression.function.ExpressionFunctionParameter;
import walkingkooka.tree.expression.function.ExpressionFunctionParameterName;

import java.util.List;

/**
 * Compares two numbers for equality returning 1 for equals and 0 for not equals.
 * Excel requires that non number parameters are a #VALUE or {@link IllegalArgumentException}.
 */
// https://exceljet.net/excel-functions/excel-delta-function
final class NumberExpressionFunctionDelta<C extends ExpressionFunctionContext> extends NumberExpressionFunction<C> {

    /**
     * Instance getter.
     */
    static <C extends ExpressionFunctionContext> NumberExpressionFunctionDelta<C> instance() {
        return Cast.to(INSTANCE);
    }

    /**
     * Singleton
     */
    private static final NumberExpressionFunctionDelta<?> INSTANCE = new NumberExpressionFunctionDelta<>();

    /**
     * Private ctor
     */
    private NumberExpressionFunctionDelta() {
        super("delta");
    }

    @Override
    public List<ExpressionFunctionParameter<?>> parameters() {
        return PARAMETERS;
    }

    private final ExpressionFunctionParameter<Object> NUMBER1 = ExpressionFunctionParameterName.with("number1")
            .required(Object.class);

    private final ExpressionFunctionParameter<Object> NUMBER2 = ExpressionFunctionParameterName.with("number2")
            .optional(Object.class);

    private final List<ExpressionFunctionParameter<?>> PARAMETERS = Lists.of(
            NUMBER1,
            NUMBER2
    );

    @Override
    public ExpressionNumber apply(final List<Object> parameters,
                                  final C context) {
        this.checkParameterCount(parameters);

        final ExpressionNumberKind kind = context.expressionNumberKind();
        final ExpressionNumber zero = kind.zero();

        final ExpressionNumber number1 = (ExpressionNumber) NUMBER1.getOrFail(parameters, 0);
        final ExpressionNumber number2 = (ExpressionNumber) NUMBER2.get(parameters, 1)
                .orElse(zero);

        return number1.equals(number2) ?
                context.expressionNumberKind().one() :
                zero;
    }
}