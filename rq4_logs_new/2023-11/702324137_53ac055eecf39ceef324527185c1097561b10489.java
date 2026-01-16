package pie.tomato.tomatomarket.application.chat;

import java.util.List;

import org.springframework.data.domain.Slice;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import lombok.RequiredArgsConstructor;
import pie.tomato.tomatomarket.infrastructure.persistence.chatroom.ChatPaginationRepository;
import pie.tomato.tomatomarket.presentation.chat.response.ChatroomListResponse;
import pie.tomato.tomatomarket.presentation.dto.CustomSlice;
import pie.tomato.tomatomarket.presentation.support.Principal;

@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class ChatroomService {

	private final ChatPaginationRepository chatPaginationRepository;

	public CustomSlice<ChatroomListResponse> findAll(Principal principal, int size, Long cursor) {
		Slice<ChatroomListResponse> responses =
			chatPaginationRepository.findAll(principal.getMemberId(), size, cursor);

		List<ChatroomListResponse> content = responses.getContent();

		Long nextCursor = content.isEmpty() ? null : content.get(content.size() - 1).getChatroomId();

		return new CustomSlice<>(content, nextCursor, responses.hasNext());
	}
}