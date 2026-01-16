package com.redhat.devtools.intellij.jkube.actions;

import com.redhat.devtools.intellij.jkube.window.TerminalLogger;
import org.eclipse.jkube.kit.remotedev.RemoteDevelopmentService;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class RemoteDevHandler {
    private final RemoteDevelopmentService service;
    private final TerminalLogger logger;

    public RemoteDevHandler(RemoteDevelopmentService service, TerminalLogger logger) {
        this.service = service;
        this.logger = logger;
    }

    public CompletableFuture<Void> start() {
        try  {
            logger.start(service);
            return service.start();
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}