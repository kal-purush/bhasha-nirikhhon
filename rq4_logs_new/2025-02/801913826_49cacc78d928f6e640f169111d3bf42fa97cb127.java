package org.example.coding.loadbalancer;

import org.example.coding.loadbalancer.model.Server;
import org.example.coding.loadbalancer.pickstrategy.ServerPickStrategy;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class LoadBalancer {

    private static final int MAX_CAPACITY = 10;

    private final Map<String, Server> urlToServerMap = new ConcurrentHashMap<>();

    private final ServerPickStrategy pickStrategy;

    public LoadBalancer(ServerPickStrategy pickStrategy) {
        if (pickStrategy == null) {
            throw new IllegalArgumentException("Pick strategy cannot be null");
        }
        this.pickStrategy = pickStrategy;
    }

    public boolean registerServer(Server server) {
        if (server == null) {
            return false;
        }

        synchronized (this) {
            if (urlToServerMap.containsKey(server.getUrl())) {
                return false;
            }

            if (urlToServerMap.size() >= MAX_CAPACITY) {
                return false;
            }

            return urlToServerMap.putIfAbsent(server.getUrl(), server) == null;
        }
    }

    public Optional<Server> getServer() {
        if (urlToServerMap.isEmpty()) {
            return Optional.empty();
        }

        return pickStrategy.pick(new ArrayList<>(urlToServerMap.values()));
    }

    public boolean removeServer(Server server) {
        if (server == null || server.getUrl() == null) {
            return false;
        }

        return urlToServerMap.remove(server.getUrl(), server);
    }

}