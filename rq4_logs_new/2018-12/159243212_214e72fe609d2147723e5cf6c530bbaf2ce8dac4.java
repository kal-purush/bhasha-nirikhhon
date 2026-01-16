package hu.bartl.bggprofileanalyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;

import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import hu.bartl.bggprofileanalyzer.configuration.EnvironmentInformations;
import hu.bartl.bggprofileanalyzer.data.BoardGame;
import hu.bartl.bggprofileanalyzer.data.CacheEntry;

import static java.lang.String.format;

@RequiredArgsConstructor
@Component
@Slf4j
public class BoardGameCache {
    
    private static final String KEY = "BoardGame";
    
    private final RedisTemplate<String, CacheEntry<BoardGame>> redisTemplate;
    private final EnvironmentInformations env;
    private HashOperations hashOperations;
    
    @PostConstruct
    private void initialize() {
        hashOperations = redisTemplate.opsForHash();
    }
    
    public void put(BoardGame boardGame) {
        hashOperations.put(KEY, boardGame.getId(), CacheEntry.of(boardGame));
    }
    
    public Optional<BoardGame> get(Integer id) {
        CacheEntry<BoardGame> cacheEntry = (CacheEntry<BoardGame>) hashOperations.get(KEY, id);
        return Optional.ofNullable(isValid(cacheEntry) ? cacheEntry.getEntry() : null);
    }
    
    public Set<BoardGame> getAll(Collection<Integer> ids) {
        Set<BoardGame> result = ((List<CacheEntry<BoardGame>>) hashOperations.multiGet(KEY, ids))
                .stream()
                .filter(this::isValid)
                .map(CacheEntry::getEntry)
                .collect(Collectors.toSet());
        log.info("Cache hit: {}/{} ({}%)", result.size(), ids.size(), format("%.02f", 100.0 * result.size() / ids.size()));
        return result;
    }
    
    private boolean isValid(CacheEntry<BoardGame> entry) {
        if (entry == null) {
            return false;
        }
        boolean isValid = entry.getInsertedAt().isAfter(Instant.now().minus(Duration.ofHours(env.getCacheExpirationInHours())));
        if (!isValid) {
            log.warn("Cached entry with inserted at {} with id {} is too old.", entry.getInsertedAt(), entry.getEntry().getId());
        }
        return isValid;
    }
}