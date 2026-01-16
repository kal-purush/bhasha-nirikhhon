package com.github.fernthedev.client.event;


import com.github.fernthedev.core.api.event.api.Event;
import com.github.fernthedev.core.api.event.api.HandlerList;
import io.netty.channel.Channel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Called when client has successfully established a secure and valid connection
 */
@RequiredArgsConstructor
public class ServerConnectFinishEvent extends Event {

    private static final HandlerList handlers = new HandlerList();

    @Getter
    private final Channel channel;

    public ServerConnectFinishEvent(Channel channel, boolean async) {
        super(async);
        this.channel = channel;
    }

    @Override
    public HandlerList getHandlers() {
        return handlers;
    }

    public static HandlerList getHandlerList() {
        return handlers;
    }
}