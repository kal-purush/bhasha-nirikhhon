package pie.tomato.tomatomarket.application.redis;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import lombok.RequiredArgsConstructor;
import pie.tomato.tomatomarket.infrastructure.persistence.item.ItemRepository;

@Service
@RequiredArgsConstructor
public class ItemViewCountRedisService {

	public static final String ITEM_VIEW_COUNT_ID_PREFIX = "itemId: ";
	private static final Pattern ITEM_ID_PATTERN = Pattern.compile("itemId:*");

	private final RedisTemplate<String, String> redisTemplate;
	private final ItemRepository itemRepository;

	public void addViewCount(String nickname, Long itemId) {
		ValueOperations<String, String> value = redisTemplate.opsForValue();

		String nicknameKey = nickname + itemId;
		if (isNotFirstView(nicknameKey)) {
			return;
		}

		String itemViewCountKey = ITEM_VIEW_COUNT_ID_PREFIX + itemId;
		if (value.get(itemViewCountKey) != null) {
			value.increment(itemViewCountKey);
			return;
		}

		value.set(nicknameKey, String.valueOf(itemId));
		value.set(itemViewCountKey, "1", Duration.ofMinutes(2));
	}

	private boolean isNotFirstView(String duplicateKey) {
		return redisTemplate.hasKey(duplicateKey);
	}

	@Transactional
	@Scheduled(cron = "0 0/1 * * * ?")
	public void deleteViewCountCache() {
		Set<String> keys = Optional.ofNullable(redisTemplate.keys(ITEM_ID_PATTERN.pattern()))
			.orElseGet(Collections::emptySet);

		for (String key : keys) {
			Long itemId = Long.parseLong(key.split(" ")[1]);
			Long viewCount = Long.valueOf(Optional.ofNullable(redisTemplate.opsForValue().get(key)).orElse("0"));
			itemRepository.findById(itemId)
				.ifPresent(item -> item.changeItemViewCount(item.getViewCount() + viewCount));
			redisTemplate.delete(key);
			redisTemplate.delete(ITEM_VIEW_COUNT_ID_PREFIX + itemId);
		}
	}
}