package com.github.codeboyzhou.mcp.declarative.server;

import com.github.codeboyzhou.mcp.declarative.configuration.McpServerCapabilities;
import com.github.codeboyzhou.mcp.declarative.configuration.McpServerChangeNotification;
import com.github.codeboyzhou.mcp.declarative.configuration.McpServerConfiguration;
import com.github.codeboyzhou.mcp.declarative.configuration.McpServerSSE;
import com.github.codeboyzhou.mcp.declarative.util.JsonHelper;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.HttpServletSseServerTransportProvider;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpServerTransportProvider;

import java.time.Duration;

public class ConfigurableMcpSyncServerFactory implements ConfigurableMcpServerFactory<McpSyncServer> {

    private final McpServerConfiguration configuration;

    public ConfigurableMcpSyncServerFactory(McpServerConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public McpSyncServer create() {
        return McpServer.sync(transportProvider())
            .instructions(configuration.instructions())
            .capabilities(configureServerCapabilities())
            .serverInfo(configuration.name(), configuration.version())
            .requestTimeout(Duration.ofMillis(configuration.requestTimeout()))
            .build();
    }

    @Override
    public McpServerTransportProvider transportProvider() {
        if (configuration.stdio()) {
            return new StdioServerTransportProvider();
        } else {
            McpServerSSE sse = configuration.sse();
            return new HttpServletSseServerTransportProvider(JsonHelper.MAPPER, sse.baseUrl(), sse.messageEndpoint(), sse.endpoint());
        }
    }

    @Override
    public McpSchema.ServerCapabilities configureServerCapabilities() {
        McpSchema.ServerCapabilities.Builder capabilities = McpSchema.ServerCapabilities.builder();
        McpServerCapabilities capabilitiesConfig = configuration.capabilities();
        McpServerChangeNotification serverChangeNotification = configuration.changeNotification();
        if (capabilitiesConfig.resource()) {
            capabilities.resources(true, serverChangeNotification.resource());
        }
        if (capabilitiesConfig.prompt()) {
            capabilities.prompts(serverChangeNotification.prompt());
        }
        if (capabilitiesConfig.tool()) {
            capabilities.tools(serverChangeNotification.tool());
        }
        return capabilities.build();
    }

}