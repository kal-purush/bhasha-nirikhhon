package thorpe.luke.ufc.fantasy.util;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Cache<TKey, TValue> {

  private final Duration timeToLiveInMinutes;
  private final Map<TKey, CacheEntry> entries;

  public Cache(Duration timeToLive) {
    this.timeToLiveInMinutes = timeToLive.truncatedTo(ChronoUnit.MINUTES);
    this.entries = new HashMap<>();
  }

  public Optional<TValue> get(TKey key) {
    CacheEntry entry = entries.get(key);
    if (entry == null || entry.isStale()) {
      entries.remove(key);
      return Optional.empty();
    }
    return Optional.of(entry.getValue());
  }

  public void add(TKey key, TValue value) {
    entries.put(key, new CacheEntry(value));
  }

  public int size() {
    return entries.size();
  }

  private class CacheEntry {

    private final LocalDateTime timestamp;
    private final TValue value;

    private CacheEntry(TValue value) {
      this.timestamp = LocalDateTime.now();
      this.value = value;
    }

    private LocalDateTime validUntil() {
      return timestamp.plus(timeToLiveInMinutes);
    }

    private boolean isStale() {
      return LocalDateTime.now().isBefore(this.validUntil());
    }

    private TValue getValue() {
      return value;
    }
  }
}