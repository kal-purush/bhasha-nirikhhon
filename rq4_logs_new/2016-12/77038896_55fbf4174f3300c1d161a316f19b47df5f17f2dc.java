package com.codingblocks.codequiz;

import com.codingblocks.codequiz.api.QuestionHandler;
import io.undertow.server.handlers.PathHandler;

/**
 * Created by championswimmer on 21/12/16.
 */
public class CodeQuizApiHandler extends PathHandler {

    private static CodeQuizApiHandler cqh;

    public static CodeQuizApiHandler getInstance () {
        if (cqh == null) {
            CodeQuizApiHandler cqh = new CodeQuizApiHandler();
            cqh.addPrefixPath("/questions", new QuestionHandler());
        }
        return cqh;
    }
}