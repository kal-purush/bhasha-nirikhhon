package com.hhi.bigdata.platform.push.server.handler;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import com.hhi.vaas.platform.middleware.client.websocket.WebSocketClientEndPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class EventHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventHandler.class);
    private WebSocketClientEndPoint adapter;

    public EventHandler() {
    }

    public abstract void handleMessageEvent(String var1);

    public abstract void handleCloseEvent(int var1, String var2);

    public abstract void handleErrorEvent(Throwable var1);

    public void setAdapter(WebSocketClientEndPoint adapter) {
        this.adapter = adapter;
    }

    public void sendAck() {
        try {
            this.adapter.getRemote().sendString("ACK");
        } catch (IOException var2) {
            LOGGER.error("IOException has occurred.", var2);
        }

    }

    public void sendNack() {
        try {
            this.adapter.getRemote().sendString("NACK");
        } catch (IOException var2) {
            LOGGER.error("IOException has occurred.", var2);
        }

    }

    public boolean isConnected() {
        return this.adapter.isConnected();
    }

    public boolean isOpen() {
        return this.adapter.getSession() != null?this.adapter.getSession().isOpen():false;
    }

    public void disconnect() throws IOException {
        if(this.adapter.getSession() != null) {
            this.adapter.getSession().disconnect();
        }

    }
}