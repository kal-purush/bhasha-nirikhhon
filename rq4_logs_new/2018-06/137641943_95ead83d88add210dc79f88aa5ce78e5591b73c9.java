package io.su0.json.path.parsing;

import io.su0.json.path.JsonPath;
import io.su0.json.path.JsonPathArraySegment;
import io.su0.json.path.JsonPathObjectSegment;

import java.util.Iterator;

public class Parser {

    public static JsonPath parse(Iterable<Token> tokens) {
        Iterator<Token> iterator = tokens.iterator();

        checkRootToken(iterator);

        JsonPath path = new JsonPath();

        while (iterator.hasNext()) {
            Token next = iterator.next();

            switch (next.getType()) {
                case DOT:
                    if (!iterator.hasNext()) {
                        throw new IllegalArgumentException("Cant finish with dot");
                    }
                    Token segmentName = iterator.next();
                    if (TokenType.OBJECT_SEGMENT != segmentName.getType()) {
                        throw new IllegalArgumentException("No object segment after dot");
                    }
                    path.add(new JsonPathObjectSegment(segmentName.getValue()));
                    break;
                case ARRAY_START:
                    if (!iterator.hasNext()) {
                        throw new IllegalArgumentException("Cant finish with array start");
                    }
                    Token arrayIndex = iterator.next();
                    if (TokenType.ARRAY_INDEX != arrayIndex.getType()) {
                        throw new IllegalArgumentException("Not array index after array start");
                    }
                    path.add(new JsonPathArraySegment(Integer.valueOf(arrayIndex.getValue())));
                    if (!iterator.hasNext()) {
                        throw new IllegalArgumentException("Cant finish with array index");
                    }
                    Token arrayEnd = iterator.next();
                    if (TokenType.ARRAY_END != arrayEnd.getType()) {
                        throw new IllegalArgumentException("Array index must follow by array end");
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Must be dot or array start");
            }
        }

        return path;
    }

    private static void checkRootToken(Iterator<Token> tokens) {
        if (!tokens.hasNext()) {
            throw new IllegalArgumentException("Empty sequence");
        }

        if (TokenType.ROOT != tokens.next().getType()) {
            throw new IllegalArgumentException("Sequence must start with root token");
        }
    }
}