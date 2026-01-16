package org.jtwig.plural.parse.parsky;

import com.google.common.base.Function;
import org.parsky.character.CharacterMatchers;
import org.parsky.sequence.SequenceMatchers;
import org.parsky.sequence.TransformSequenceMatcher;
import org.parsky.sequence.transform.ContentTransformation;
import org.parsky.sequence.transform.Transformations;

import java.util.regex.Pattern;

public class MessageSequenceMatcher extends TransformSequenceMatcher<String> {
    public MessageSequenceMatcher() {
        super(SequenceMatchers.matchedText(
                SequenceMatchers.zeroOrMore(
                        SequenceMatchers.firstOf(
                                SequenceMatchers.string("\\|"),
                                SequenceMatchers.not(
                                        SequenceMatchers.firstOf(
                                                SequenceMatchers.string("|"),
                                                SequenceMatchers.match(CharacterMatchers.endOfInput())
                                        )
                                )
                        )
                )
        ), Transformations.fromString(new Function<ContentTransformation.Request<String>, String>() {
            @Override
            public String apply(ContentTransformation.Request<String> input) {
                return input.getValue().replaceAll(Pattern.quote("\\|"), "|").trim();
            }
        }));
    }
}