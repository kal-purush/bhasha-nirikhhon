
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
import walkingkooka.tree.expression.ExpressionNumber;
import walkingkooka.tree.expression.function.ExpressionFunctionContext;
import walkingkooka.tree.expression.function.ExpressionFunctionParameter;

import java.util.List;
import java.util.function.Function;

/**
 * Functions that return constants like E and PI.
 */
final class NumberExpressionFunctionConstants<C extends ExpressionFunctionContext> extends NumberExpressionFunction<C> {

    /**
     * e Instance getter.
     */
    static <C extends ExpressionFunctionContext> NumberExpressionFunctionConstants<C> e() {
        return Cast.to(E);
    }

    private static final NumberExpressionFunctionConstants<?> E = new NumberExpressionFunctionConstants<>(
            "e",
            (c) -> c.expressionNumberKind().e(c)
    );

    /**
     * e Instance getter.
     */
    static <C extends ExpressionFunctionContext> NumberExpressionFunctionConstants<C> pi() {
        return Cast.to(PI);
    }

    private static final NumberExpressionFunctionConstants<?> PI = new NumberExpressionFunctionConstants<>(
            "pi",
            (c) -> c.expressionNumberKind().pi(c)
    );

    private NumberExpressionFunctionConstants(final String name,
                                              final Function<ExpressionFunctionContext, ExpressionNumber> getter) {
        super(name);
        this.getter = getter;
    }

    @Override
    public List<ExpressionFunctionParameter<?>> parameters() {
        return ExpressionFunctionParameter.EMPTY;
    }

    @Override
    public ExpressionNumber apply(final List<Object> parameters,
                                  final C context) {
        this.checkParameterCount(parameters);

        return this.getter.apply(context);
    }

    private final Function<ExpressionFunctionContext, ExpressionNumber> getter;
}