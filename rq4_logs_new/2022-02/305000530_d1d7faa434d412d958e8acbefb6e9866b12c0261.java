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

import walkingkooka.tree.expression.FakeExpressionNumberContext;

import java.math.MathContext;

final class NumberExpressionFunctionUnaryRoundExpressionNumberContext extends FakeExpressionNumberContext {

    static NumberExpressionFunctionUnaryRoundExpressionNumberContext with(final MathContext mathContext) {
        return new NumberExpressionFunctionUnaryRoundExpressionNumberContext(mathContext);
    }

    private NumberExpressionFunctionUnaryRoundExpressionNumberContext(final MathContext mathContext) {
        this.mathContext = mathContext;
    }

    @Override
    public MathContext mathContext() {
        return this.mathContext;
    }

    private final MathContext mathContext;

    @Override
    public String toString() {
        return this.mathContext.toString();
    }
}