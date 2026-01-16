package pie.tomato.tomatomarket.presentation.response.item;

import java.time.LocalDateTime;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import pie.tomato.tomatomarket.domain.Item;

@Getter
@AllArgsConstructor
public class ItemDetailResponse {

	private String nickname;
	private String status;
	private String title;
	private String thumbnail;
	private List<String> images;
	private LocalDateTime createdAt;
	private String content;
	private Long price;
	private Long chatCount;
	private Long wishCount;
	private Long viewCount;
	private boolean isInWishList;

	public static ItemDetailResponse toEntity(Item item, boolean isInWishList, List<String> images) {
		return new ItemDetailResponse(
			item.getMember().getNickname(),
			item.getStatus().getName(),
			item.getTitle(),
			item.getThumbnail(),
			images,
			item.getCreatedAt(),
			item.getContent(),
			item.getPrice(),
			item.getChatCount(),
			item.getWishCount(),
			item.getViewCount(),
			isInWishList);
	}
}