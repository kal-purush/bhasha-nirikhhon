package org.jtwig.plural;

import com.google.common.base.Optional;
import org.jtwig.plural.model.PluralOption;
import org.jtwig.plural.parse.parboiled.ParserContext;
import org.jtwig.plural.parse.parboiled.PluralOptionsParser;
import org.parboiled.errors.ParserRuntimeException;
import org.parboiled.parserunners.BasicParseRunner;
import org.parboiled.support.ParsingResult;

import java.util.Collection;

import static org.parboiled.Parboiled.createParser;

public class PluralOptions {
    public static PluralOptions parse (String representation) {
        PluralOptionsParser parser = createParser(PluralOptionsParser.class, new ParserContext());

        BasicParseRunner<Collection<PluralOption>> runner = new BasicParseRunner<Collection<PluralOption>>(parser.Rule());

        try {
            ParsingResult<Collection<PluralOption>> result = runner.run(representation);
            return new PluralOptions(result.resultValue);
        } catch (ParserRuntimeException e) {
            if (e.getCause() instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) e.getCause();
            }
            throw e;
        }
    }

    private final Collection<PluralOption> pluralOption;

    public PluralOptions(Collection<PluralOption> pluralOption) {
        this.pluralOption = pluralOption;
    }

    public Optional<String> select (int value) {
        for (PluralOption option : pluralOption) {
            if (option.apply(value)) {
                return Optional.of(option.getMessage());
            }
        }
        return Optional.absent();
    }
}