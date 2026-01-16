package com.github.alex_moon.porcupi.handlers;

import java.util.List;

public abstract class Context implements Handler {
    protected Boolean isBlocking = true;
    protected long threadId;
    protected Handler handler;

    public Context(long threadId, Handler handler) {
        this.threadId = threadId;
        this.handler = handler;
    }
    
    public Boolean canHandle(long threadId, String key) {
        if (threadId == this.threadId) {
            return canHandle(key);
        }
        return false;
    }
    
    public Boolean canHandle(String key) {
        return false;
    }
    
    public String handle(String key, List<String> tokens) {
        return null;
    }
    
    public String handle(long threadId, String key, List<String> tokens) {
        if (threadId == this.threadId) {
            return handle(key, tokens);
        }
        return null;
    }
}