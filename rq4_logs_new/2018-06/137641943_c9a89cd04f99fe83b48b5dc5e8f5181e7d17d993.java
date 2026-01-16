package io.su0.json.parser;

import io.su0.json.parser.walker.HandlerStorage;
import io.su0.json.parser.walker.JsonTreeWalker;

import java.io.IOException;
import java.io.InputStream;

public abstract class AbstractJsonHandler {

    private final HandlerStorage handlerStorage;

    public AbstractJsonHandler() {
        this.handlerStorage = new HandlerStorage();
    }

    protected void handleInputStream(InputStream inputStream) throws IOException {
        JsonTreeWalker.walk(inputStream, handlerStorage);
    }
}