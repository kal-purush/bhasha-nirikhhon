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

// https://exceljet.net/excel-functions/excel-decimal-function
final class NumberExpressionFunctionDecimal<C extends ExpressionFunctionContext> extends NumberExpressionFunction<C> {

    /**
     * Instance getter.
     */
    static <C extends ExpressionFunctionContext> NumberExpressionFunctionDecimal<C> instance() {
        return Cast.to(INSTANCE);
    }

    /**
     * Singleton
     */
    private static final NumberExpressionFunctionDecimal<?> INSTANCE = new NumberExpressionFunctionDecimal<>();

    /**
     * Private ctor
     */
    private NumberExpressionFunctionDecimal() {
        super("decimal");
    }

    @Override
    public List<ExpressionFunctionParameter<?>> parameters() {
        return PARAMETERS;
    }

    private final ExpressionFunctionParameter<String> TEXT = ExpressionFunctionParameter.TEXT;

    private final ExpressionFunctionParameter<ExpressionNumber> BASE = ExpressionFunctionParameterName.with("base")
            .required(ExpressionNumber.class);

    private final List<ExpressionFunctionParameter<?>> PARAMETERS = Lists.of(
            TEXT,
            BASE
    );

    @Override
    public ExpressionNumber apply(final List<Object> parameters,
                                  final C context) {
        this.checkParameterCount(parameters);

        return context.expressionNumberKind()
                .parseWithBase(
                        TEXT.getOrFail(parameters, 0),
                        BASE.getOrFail(parameters, 1).intValueExact(),
                        context
                );
    }
}