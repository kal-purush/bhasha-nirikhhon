package com.and1ss.onlinechat.repositories.mappers;

import com.and1ss.onlinechat.repositories.projections.GroupChatWithLastMessageProjection;
import com.and1ss.onlinechat.services.dto.AccountInfoRetrievalDTO;
import com.and1ss.onlinechat.services.dto.GroupChatRetrievalDTO;
import com.and1ss.onlinechat.services.dto.GroupMessageRetrievalDTO;

import java.util.UUID;

import static com.and1ss.onlinechat.utils.DatabaseQueryHelper.getUUIDFromStringOrNull;

public class GroupChatProjectionsMapper {
    public static GroupChatRetrievalDTO toGroupChatRetrievalDTO(GroupChatWithLastMessageProjection projection) {
        final UUID id = getUUIDFromStringOrNull(projection.getChatId());
        final UUID creatorId = getUUIDFromStringOrNull(projection.getChatCreatorId());
        final UUID lastMessageId = getUUIDFromStringOrNull(projection.getLastMessageId());
        final UUID lastMessageAuthorId = getUUIDFromStringOrNull(projection.getLastMessageAuthorId());

        final var groupChatBuilder = GroupChatRetrievalDTO.builder();

        if (id == null || projection.getChatTitle() == null) return null;

        groupChatBuilder.id(id);
        groupChatBuilder.title(projection.getChatTitle());
        groupChatBuilder.about(projection.getChatAbout());

        if (creatorId == null || projection.getChatAbout() == null || projection.getChatCreatorName() == null
                || projection.getChatCreatorSurname() == null || projection.getChatCreatorLogin() == null
        ) {
            groupChatBuilder.creator(null);
        } else {
            final var creator = AccountInfoRetrievalDTO.builder()
                    .id(creatorId)
                    .name(projection.getChatCreatorName())
                    .surname(projection.getChatCreatorSurname())
                    .login(projection.getChatCreatorLogin())
                    .build();
            groupChatBuilder.creator(creator);
        }

        if (lastMessageId == null || projection.getLastMessageContents() == null
                || projection.getLastMessageCreationTime() == null) {
            groupChatBuilder.lastMessage(null);
        } else {
            final var lastMessageBuilder = GroupMessageRetrievalDTO.builder();
            lastMessageBuilder.id(lastMessageId);
            lastMessageBuilder.contents(projection.getLastMessageContents());
            lastMessageBuilder.chatId(id);
            lastMessageBuilder.createdAt(projection.getLastMessageCreationTime());

            if (lastMessageAuthorId == null
                    || projection.getLastMessageAuthorName() == null
                    || projection.getLastMessageAuthorSurname() == null
                    || projection.getLastMessageAuthorLogin() == null
            ) {
                lastMessageBuilder.author(null);
            } else {
                final var lastMessageAuthor = AccountInfoRetrievalDTO.builder()
                        .id(lastMessageAuthorId)
                        .name(projection.getLastMessageAuthorName())
                        .surname(projection.getLastMessageAuthorSurname())
                        .login(projection.getLastMessageAuthorLogin())
                        .build();
                lastMessageBuilder.author(lastMessageAuthor);
            }

            groupChatBuilder.lastMessage(lastMessageBuilder.build());
        }

        return groupChatBuilder.build();
    }
}