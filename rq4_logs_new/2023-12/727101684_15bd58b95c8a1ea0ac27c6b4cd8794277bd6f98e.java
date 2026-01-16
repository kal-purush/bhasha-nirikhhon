package com.example.demo.boardapi.dto;

import com.example.demo.boardapi.entity.Board;
import com.example.demo.userapi.entity.User;
import lombok.*;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

@Getter @Setter
@ToString @EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BoardUpdateRequestDTO {

    @NotBlank
    private int boardId;

    @NotBlank
    @Size(min = 2, max = 30)
    private String title;

    private String content;

    private String category; // 사용자가 선택한 카테고리

    // dto를 엔티티로 변환
    public Board toEntity(User user){
        return Board.builder()
                .category(this.category)
                .title(this.title)
                .content(this.content)
                .user(user)
                .build();
    }


}