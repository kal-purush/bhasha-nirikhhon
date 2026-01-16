package com.github.codeboyzhou.mcp.declarative.server.register;

import com.google.inject.Injector;
import io.modelcontextprotocol.server.McpSyncServer;

public class McpServerComponentRegisters {

    private final Injector injector;

    public McpServerComponentRegisters(Injector injector) {
        this.injector = injector;
    }

    public void registerAllTo(McpSyncServer server) {
        new McpSyncServerResourceRegister(injector).registerTo(server);
        new McpSyncServerPromptRegister(injector).registerTo(server);
        new McpSyncServerToolRegister(injector).registerTo(server);
    }

}