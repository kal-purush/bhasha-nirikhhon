package org.example.coding.urlshortener;

import org.example.coding.urlshortener.keygeneratorstrategy.KeyGeneratorStrategy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class UrlShortenerService {

    private static final String URL_PREFIX = "https://short.ly/";

    private final Map<String, String> shortToLongMapping = new ConcurrentHashMap<>();

    private final Lock lock = new ReentrantLock();

    private final KeyGeneratorStrategy keyGeneratorStrategy;

    public UrlShortenerService(KeyGeneratorStrategy keyGeneratorStrategy) {
        this.keyGeneratorStrategy = keyGeneratorStrategy;
    }

    public String shortenUrl(String url) {
        if (url == null || url.isEmpty()) {
            return null;
        }

        try {
            lock.lock(); // can be used just synchronized (this) instead of lock - lock was used for testing purposes

            String existingShortUrl = shortToLongMapping.entrySet().stream()
                    .filter(entry -> url.equals(entry.getValue()))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(null);
            if (existingShortUrl != null) {
                return existingShortUrl;
            }

            String shortUrl = URL_PREFIX + keyGeneratorStrategy.generate();

            shortToLongMapping.putIfAbsent(shortUrl, url);

            return shortUrl;
        } finally {
            lock.unlock();
        }
    }

    public String getUrl(String shortenedUrl) {
        return shortToLongMapping.get(shortenedUrl);
    }

}