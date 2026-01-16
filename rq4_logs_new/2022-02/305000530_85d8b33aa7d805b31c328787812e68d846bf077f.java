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

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.List;

// https://exceljet.net/excel-functions/excel-fixed-function
final class StringExpressionFunctionFixed<C extends ExpressionFunctionContext> extends StringExpressionFunction<C> {

    /**
     * Instance getter.
     */
    static <C extends ExpressionFunctionContext> StringExpressionFunctionFixed<C> instance() {
        return Cast.to(INSTANCE);
    }

    /**
     * Singleton
     */
    private static final StringExpressionFunctionFixed<?> INSTANCE = new StringExpressionFunctionFixed<>();

    /**
     * Private ctor
     */
    private StringExpressionFunctionFixed() {
        super("fixed");
    }

    @Override
    public List<ExpressionFunctionParameter<?>> parameters() {
        return PARAMETERS;
    }

    private final static ExpressionFunctionParameter<ExpressionNumber> NUMBER = ExpressionFunctionParameter.NUMBER;

    private final static ExpressionFunctionParameter<ExpressionNumber> DECIMALS = ExpressionFunctionParameterName.with("decimals")
            .optional(ExpressionNumber.class);

    private final static ExpressionFunctionParameter<Boolean> COMMAS = ExpressionFunctionParameterName.with("commas")
            .optional(Boolean.class);

    private final static List<ExpressionFunctionParameter<?>> PARAMETERS = Lists.of(
            NUMBER,
            DECIMALS,
            COMMAS
    );

    // https://github.com/apache/poi/blob/7eaca60a1a364ce6e232363d27823e971a992705/poi/src/main/java/org/apache/poi/ss/formula/functions/Roman.java

    @Override
    public String apply(final List<Object> parameters,
                        final C context) {
        this.checkParameterCount(parameters);

        final ExpressionNumber number = NUMBER.getOrFail(parameters, 0);
        final int decimals = DECIMALS.get(parameters, 1)
                .orElseGet(() -> context.expressionNumberKind().create(2))
                .intValueExact();
        final Boolean commas = COMMAS.get(parameters, 2).orElse(false);

        final DecimalFormatSymbols symbols = new DecimalFormatSymbols();

        symbols.setMinusSign(context.negativeSign());

        symbols.setDecimalSeparator(context.decimalSeparator());
        symbols.setGroupingSeparator(context.groupingSeparator());


        final DecimalFormat format = new DecimalFormat("", symbols);
        format.setRoundingMode(context.mathContext().getRoundingMode());

        format.setMaximumFractionDigits(decimals);
        format.setMinimumFractionDigits(decimals);

        format.setDecimalSeparatorAlwaysShown(decimals > 0);

        format.setGroupingUsed(!commas);

        return format.format(number.bigDecimal()); // TODO should test if BigDecimal or Double
    }
}