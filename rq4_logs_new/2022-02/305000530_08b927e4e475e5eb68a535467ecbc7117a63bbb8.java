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
import walkingkooka.tree.expression.function.ExpressionFunctionContext;
import walkingkooka.tree.expression.function.ExpressionFunctionParameter;
import walkingkooka.tree.expression.function.ExpressionFunctionParameterName;

import java.util.List;

/**
 * Returns the product for the given parameters.
 */
// https://exceljet.net/excel-functions/excel-quotient-function
final class NumberExpressionFunctionProduct<C extends ExpressionFunctionContext> extends NumberExpressionFunction<C> {

    /**
     * Instance getter.
     */
    static <C extends ExpressionFunctionContext> NumberExpressionFunctionProduct<C> instance() {
        return Cast.to(INSTANCE);
    }

    /**
     * Singleton
     */
    private static final NumberExpressionFunctionProduct<?> INSTANCE = new NumberExpressionFunctionProduct<>();

    private NumberExpressionFunctionProduct() {
        super("product");
    }

    @Override
    public List<ExpressionFunctionParameter<?>> parameters() {
        return PARAMETERS;
    }

    private final static ExpressionFunctionParameter<ExpressionNumber> NUMBER1 = ExpressionFunctionParameterName.with("number1")
            .required(ExpressionNumber.class);

    private final static ExpressionFunctionParameter<ExpressionNumber> NUMBER2 = ExpressionFunctionParameterName.with("number2")
            .required(ExpressionNumber.class);

    private final static List<ExpressionFunctionParameter<?>> PARAMETERS = Lists.of(
            NUMBER1,
            NUMBER2
    );

    @Override
    public ExpressionNumber apply(final List<Object> parameters,
                                  final C context) {
        this.checkParameterCount(parameters);

        final ExpressionNumber number1 = NUMBER1.getOrFail(parameters, 0);
        final ExpressionNumber number2 = NUMBER2.getOrFail(parameters, 1);

        return number1.multiply(number2, context);
    }
}