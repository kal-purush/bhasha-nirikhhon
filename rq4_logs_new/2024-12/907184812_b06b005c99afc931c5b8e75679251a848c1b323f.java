package tabom.myhands.domain.schedule.dto;

import lombok.*;
import tabom.myhands.domain.schedule.entity.Candidate;
import tabom.myhands.domain.user.entity.User;

public class CandidateResponse {
    @Getter
    @Builder
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Detail {
        private Long userId;
        private String photo;

        public static CandidateResponse.Detail build(User user){
            return Detail.builder()
                    .userId(user.getUserId())
                    .photo(user.getPhoto())
                    .build();
        }
    }
}