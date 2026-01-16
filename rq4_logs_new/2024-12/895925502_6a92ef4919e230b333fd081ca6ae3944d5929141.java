package com.mulmeong.notification.application;

import com.mulmeong.event.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumer {
    @KafkaListener(topics = "${event.notification.pub.topics.chatbot-chatting-create.name}",
            containerFactory = "chatbotChattingCreateListener")
    public void chatBotChattingCreated(ChatBotChattingCreateEvent message) {
        System.out.println(message.getMessage());
    }

    @KafkaListener(topics = "${event.notification.pub.topics.feed-comment-create.name}",
            containerFactory = "feedCommentCreateListener")
    public void createFeedComment(FeedCommentCreateEvent message) {
        System.out.println(message.getContent());
    }

    @KafkaListener(topics = "${event.notification.pub.topics.feed-recomment-create.name}",
            containerFactory = "feedRecommentCreateListener")
    public void createFeedRecomment(FeedRecommentCreateEvent message) {
        System.out.println(message.getContent());
    }

    @KafkaListener(topics = "${event.notification.pub.topics.shorts-comment-create.name}",
            containerFactory = "shortsCommentCreateListener")
    public void createShortsComment(ShortsCommentCreateEvent message) {
        System.out.println(message.getContent());
    }

    @KafkaListener(topics = "${event.notification.pub.topics.shorts-recomment-create.name}",
            containerFactory = "shortsRecommentCreateListener")
    public void createShortsRecomment(ShortsRecommentCreateEvent message) {
        System.out.println(message.getContent());
    }
}