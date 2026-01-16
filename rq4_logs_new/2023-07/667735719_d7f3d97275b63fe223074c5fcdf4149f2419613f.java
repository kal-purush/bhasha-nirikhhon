package com.example.youtubeClone.dto;

import lombok.Data;

/**
 * @ Id : 답글의 아이디
 * @ parentId : 부모 댓글의 아이디
 * @ boardId : 댓글이 달린 게시판 아이디
 * @ cotent : 댓글 내용
 * @ username : 작정자 명
 * @ dateTime : 댓글이 작성된 시간
 */
@Data
public class Reply {
    Long id;
    Long parentId;
    Long boardId;
    String content;
    String username;
    String dateTime;

    @Override
    public String toString() {
        return "Reply{" +
                "id=" + id +
                ", parentId=" + parentId +
                ", boardId=" + boardId +
                ", content='" + content + '\'' +
                ", username='" + username + '\'' +
                ", dateTime='" + dateTime + '\'' +
                '}';
    }
}