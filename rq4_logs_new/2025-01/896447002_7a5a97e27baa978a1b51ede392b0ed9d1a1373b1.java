package com.example.musing.board.dto;

import com.example.musing.reply.dto.ReplyDto;
import lombok.Builder;
import org.springframework.data.domain.Page;

@Builder
public record BoardAndReplyPageDto(
        BoardRequestDto.BoardDto boardDto,
        Page<ReplyDto> replyDtos
) {
    public static BoardAndReplyPageDto of(BoardRequestDto.BoardDto boardDto, Page<ReplyDto> replyDtos){
        return BoardAndReplyPageDto.builder()
                .boardDto(boardDto)
                .replyDtos(replyDtos)
                .build();
    }
}