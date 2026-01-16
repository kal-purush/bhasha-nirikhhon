package com.mulmeong.notification.application;

import com.mulmeong.notification.client.comment.*;
import com.mulmeong.notification.client.feed.FeedClient;
import com.mulmeong.notification.client.feed.FeedDto;
import com.mulmeong.notification.client.member.MemberClient;
import com.mulmeong.notification.client.member.MemberDto;
import com.mulmeong.notification.client.shorts.ShortsClient;
import com.mulmeong.notification.client.shorts.ShortsDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class FeignService {

    private final MemberClient memberClient;
    private final FeedClient feedClient;
    private final ShortsClient shortsClient;
    private final CommentClient commentClient;

    MemberDto getCompactProfileByUuid(String memberUuid) {
        return memberClient.getCompactProfileByUuid(memberUuid).result();
    }

    FeedDto getSingleFeed(String feedUuid) {
        return feedClient.getSingleFeed(feedUuid).result();
    }

    ShortsDto getSingleShorts(String shortsUuid) {
        return shortsClient.getSingleShorts(shortsUuid).result();
    }

    FeedCommentDto getFeedComment(String commentUuid) {
        return commentClient.getFeedComment(commentUuid).result();
    }

    FeedRecommentDto getFeedRecomment(String recommentUuid) {
        return commentClient.getFeedRecomment(recommentUuid).result();
    }

    ShortsCommentDto getShortsComment(String commentUuid) {
        return commentClient.getShortsComment(commentUuid).result();
    }

    ShortsRecommentDto getShortsRecomment(String recommentUuid) {
        return commentClient.getShortsRecomment(recommentUuid).result();
    }

}