package com.sharework.common;

import lombok.Getter;

@Getter
public enum AlarmTypeEnum {
    JOB_APPLICATION_RECEIVED(
        "[%s] 님으로부터 구직 신청이 들어왔습니다.",
        "채택 여부를 확인해주세요."
    ), // 구직신청 (업주한테 알림)

    SELECTED(
        "[%s] 에 채택되었습니다.",
        "지각하지 말고 일해주세요."
    ), // 채택 (알바한테 알림)

    DESELECTED(
        "[%s] 에 채택이 취소되었습니다.",
        "다른곳에 다시 지원하세요."
    ), // 업주가 알바생 채택 취소 (알바한테 알림)

    JOB_START_REQUESTED(
        "[%s] 님이 알바시작을 요청하였습니다.",
        "도착했다면 수락해주세요."
    ), // 알바시작 요청 (업주한테 알림)

    JOB_RECRUIT_CLOSED(
        "구인마감",
        "아무도 채택되지 않아서 구인이 자동으로 마감되었습니다."
    ), // 구인 FAILED (업주한테 알림)

    JOB_FINISHED(
        "일 완료",
        "일이 완료되었습니다."
    ), // 구인 완료 (업주한테 알림)

    JOB_DONE(
        "알바완료",
        "고생하셨습니다."
    ); // 알바 완료 (알바한테 알림)

    private final String title;
    private final String message;

    AlarmTypeEnum(String title, String message) {
        this.title = title;
        this.message = message;
    }
}