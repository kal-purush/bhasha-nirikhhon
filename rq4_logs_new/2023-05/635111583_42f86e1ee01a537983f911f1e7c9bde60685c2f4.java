package com.example.board.domain.form;

import com.example.board.domain.type.RankType;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


public class RankUpStandardForm {

    @Getter
    @Setter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AddRankUpStandard {

        @NotNull(message = "등급을 확인해주세요.")
        @Enumerated(EnumType.STRING)
        private RankType rankName;
        @NotNull(message = "게시글 작성 수를 입력하세요.")
        @Min(value = 0, message = "게시글 작성 수는 0이상 입니다.")
        private Long boardCount;
        @NotNull(message = "댓글 작성 수를 입력하세요.")
        @Min(value = 0, message = "댓글 작성 수는 0이상 입니다.")
        private Long commentCount;
        @NotNull(message = "자동 등업 여부를 선택하세요.")
        private boolean autoRankUpFlag;
    }


    @Getter
    @Setter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ModifyRankUpStandard {

        @NotNull(message = "게시글 작성 수를 입력하세요.")
        @Min(value = 0, message = "게시글 작성 수는 0이상 입니다.")
        private Long boardCount;
        @NotNull(message = "댓글 작성 수를 입력하세요.")
        @Min(value = 0, message = "댓글 작성 수는 0이상 입니다.")
        private Long commentCount;
        @NotNull(message = "자동 등업 여부를 선택하세요.")
        private boolean autoRankUpFlag;
    }
}