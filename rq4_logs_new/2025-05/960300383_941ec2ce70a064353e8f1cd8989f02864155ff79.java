package com.github.codeboyzhou.mcp.declarative.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;

public class YAMLConfigurationLoader {

    private static final Logger logger = LoggerFactory.getLogger(YAMLConfigurationLoader.class);

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private static final String DEFAULT_CONFIG_FILE_NAME = "mcp-server-default.yml";

    private static final String CONFIG_FILE_NAME = "mcp-server.yml";

    private static final String WATCH_THREAD_NAME = "McpServerConfigFileWatcher";

    private final String configFileName;

    private WatchService watchService;

    private McpServerConfiguration config;

    public YAMLConfigurationLoader(String configFileName) {
        this.configFileName = configFileName;
        initializeWatchService();
        loadConfig();
    }

    public YAMLConfigurationLoader() {
        this(CONFIG_FILE_NAME);
    }

    public McpServerConfiguration getConfig() {
        return config;
    }

    private Path getConfigFilePath(String fileName) {
        try {
            ClassLoader classLoader = YAMLConfigurationLoader.class.getClassLoader();
            URL configFileUrl = classLoader.getResource(fileName);
            if (configFileUrl == null) {
                configFileUrl = classLoader.getResource(DEFAULT_CONFIG_FILE_NAME);
            }
            assert configFileUrl != null;
            return Paths.get(configFileUrl.toURI());
        } catch (URISyntaxException e) {
            // should never happen
            return null;
        }
    }

    private void initializeWatchService() {
        try {
            Path configFilePath = getConfigFilePath(configFileName);
            assert configFilePath != null;
            Path parentPath = configFilePath.getParent();
            watchService = FileSystems.getDefault().newWatchService();
            parentPath.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

            Thread watchThread = new Thread(this::watchConfigFile, WATCH_THREAD_NAME);
            watchThread.setDaemon(true);
            watchThread.start();
        } catch (IOException e) {
            logger.error("Failed to initialize configuration file watch service", e);
        }
    }

    private void watchConfigFile() {
        try {
            while (true) {
                WatchKey watchKey = watchService.take();
                List<WatchEvent<?>> watchEvents = watchKey.pollEvents();
                for (WatchEvent<?> event : watchEvents) {
                    if (event.context().toString().equals(configFileName)) {
                        loadConfig();
                    }
                }
                watchKey.reset();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Configuration file watch service interrupted", e);
        }
    }

    private void loadConfig() {
        try {
            Path configFilePath = getConfigFilePath(configFileName);
            assert configFilePath != null;
            config = YAML_MAPPER.readValue(configFilePath.toFile(), McpServerConfiguration.class);
            logger.info("Configuration loaded successfully");
        } catch (IOException e) {
            logger.error("Failed to reload configuration", e);
        }
    }

}