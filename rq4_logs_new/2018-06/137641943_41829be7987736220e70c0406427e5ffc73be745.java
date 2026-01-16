package io.su0.json.path.parsing;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Lexer implements Iterable<Token> {

    private class MatcherFindGuard {

        private final Matcher matcher;
        private boolean findCalled = false;
        private boolean lastFind;

        public MatcherFindGuard(Matcher matcher) {
            this.matcher = matcher;
        }

        public boolean find() {
            if (!findCalled) {
                lastFind = matcher.find();
                findCalled = true;
            }
            return lastFind;
        }

        public void reset() {
            findCalled = false;
        }
    }

    private class LexerIterator implements Iterator<Token> {

        private final Matcher matcher;
        private final MatcherFindGuard guard;

        public LexerIterator(String string) {
            String loopPattern = Arrays.stream(TokenType.values())
                    .map(tokenType -> String.format("(?<%s>%s)", tokenType.groupName, tokenType.pattern))
                    .collect(Collectors.joining("|"));

            this.matcher = Pattern.compile(loopPattern).matcher(string);
            this.guard = new MatcherFindGuard(matcher);
        }

        @Override
        public boolean hasNext() {
            return guard.find();
        }

        @Override
        public Token next() {
            if (!guard.find()) {
                throw new NoSuchElementException();
            }
            guard.reset();

            for (TokenType token : TokenType.values()) {
                String group = matcher.group(token.groupName);
                if (Objects.nonNull(group)) {
                    return new Token(token, token.hasValue ? group : null);
                }
            }
            throw new IllegalStateException(String.format("No token matched at %d", matcher.end()));
        }
    }

    private final String string;

    public Lexer(String string) {
        this.string = string;
    }

    @Override
    public Iterator<Token> iterator() {
        return new LexerIterator(string);
    }
}