package com.mulmeong.notification.document;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum NotificationComment {

    FEED_COMMENT_CREATE("회원님의 피드에 댓글을 남겼습니다."),
    SHORTS_COMMENT_CREATE("회원님의 쇼츠에 댓글을 남겼습니다."),
    FEED_LIKE_CREATE("회원님의 피드를 좋아합니다."),
    SHORTS_LIKE_CREATE("회원님의 쇼츠를 좋아합니다."),
    COMMENT_LIKE_CREATE("회원님의 댓글을 좋아합니다."),
    RECOMMENT_LIKE_CREATE("회원님의 대댓글을 좋아합니다."),
    FOLLOW_CREATE("회원님을 팔로우했습니다."),
    FEED_CREATE("새로운 피드를 생성했습니다."), //팔로우한 사람의 새로운 피드 생성
    SHORTS_CREATE("새로운 쇼츠를 생성했습니다."), //팔로우한 사람의 새로운 쇼츠 생성
    GRADE_UPDATE("회원님의 등급이 변경되었습니다."),
    WIN_CONTEST("콘테스트 결과가 나왔습니다. 등수를 확인하세요."),
    REPORT_CREATE("신고가 접수되었습니다."),
    CHAT_CREATE("메세지를 전송했습니다.");

    private final String comment;

}