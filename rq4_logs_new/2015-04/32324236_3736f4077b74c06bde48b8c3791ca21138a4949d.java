package com.github.alex_moon.porcupi.manager.context;

import java.util.List;

import com.github.alex_moon.porcupi.handlers.Handler;

public abstract class Context implements Handler {
    protected Boolean isServer = true;
    protected Boolean isBlocking = true;
    protected long threadId;

    public Context() throws ContextException {
        throw new ContextException("Not enough args to constructor");
    }

    public Context(long threadId, Boolean isServer) {
        this.threadId = threadId;
        this.isServer = isServer;
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
        if (isServer) {
            return serverHandle(key, tokens);
        }
        return clientHandle(key, tokens);
    }
    
    public String serverHandle(String key, List<String> tokens) {
        return null;
    }

    public String clientHandle(String key, List<String> tokens) {
        return null;
    }
        
}