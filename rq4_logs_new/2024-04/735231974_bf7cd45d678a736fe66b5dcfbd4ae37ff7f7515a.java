package com.bodytok.healthdiary.dto.diaryLike;

import com.bodytok.healthdiary.domain.DiaryLike;
import com.bodytok.healthdiary.domain.PersonalExerciseDiary;
import com.bodytok.healthdiary.domain.UserAccount;

import java.util.Set;
import java.util.stream.Collectors;

public record DiaryLikeInfo(
        Set<Long> userIds,
        Integer totalCount
) {

    public static DiaryLikeInfo from(PersonalExerciseDiary diary) {
        return new DiaryLikeInfo(
                diary.getLikes().stream()
                        .map(DiaryLike::getUserAccount)
                        .map(UserAccount::getId).collect(Collectors.toUnmodifiableSet()),
                diary.getLikes().size()
        );
    }
}