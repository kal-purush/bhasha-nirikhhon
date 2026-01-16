package com.mulmeong.notification.application;

import com.mulmeong.event.chat.ChattingCreateEvent;
import com.mulmeong.event.contents.*;
import com.mulmeong.event.member.MemberGradeUpdateEvent;
import com.mulmeong.event.contest.ContestVoteResultEvent;
import com.mulmeong.event.member.FollowCreateEvent;
import com.mulmeong.event.report.ReportApproveEvent;
import com.mulmeong.notification.document.NotificationHistory;

public interface EventProcessService {

    NotificationHistory saveFeedEvent(FeedCreatedFollowersEvent message);

    NotificationHistory saveShortsEvent(ShortsCreatedFollowersEvent message);

    NotificationHistory saveFeedCommentEvent(FeedCommentCreateEvent message);

    NotificationHistory saveFeedRecommentEvent(FeedRecommentCreateEvent message);

    NotificationHistory saveShortsCommentEvent(ShortsCommentCreateEvent message);

    NotificationHistory saveShortsRecommentEvent(ShortsRecommentCreateEvent message);

    NotificationHistory saveLikeEvent(LikeCreateEvent message);

    NotificationHistory saveFollowEvent(FollowCreateEvent message);

    NotificationHistory saveChattingEvent(ChattingCreateEvent message);

    NotificationHistory saveContestResultEvent(ContestVoteResultEvent message);

    NotificationHistory saveMemberGradeEvent(MemberGradeUpdateEvent message);

    NotificationHistory saveReportEvent(ReportApproveEvent message);
}