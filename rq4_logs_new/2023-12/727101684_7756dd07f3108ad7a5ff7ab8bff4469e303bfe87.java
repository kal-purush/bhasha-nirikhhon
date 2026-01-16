package com.example.demo.qnaapi.api;

import com.example.demo.auth.TokenUserInfo;
import com.example.demo.qnaapi.dto.request.BoardCreateRequestDTO;
import com.example.demo.qnaapi.dto.request.BoardModifyRequestDTO;
import com.example.demo.qnaapi.dto.request.CommentCreateRequestDTO;
import com.example.demo.qnaapi.dto.request.CommentModifyRequestDTO;
import com.example.demo.qnaapi.dto.response.BoardDetailResponseDTO;
import com.example.demo.qnaapi.dto.response.BoardListResponseDTO;
import com.example.demo.qnaapi.dto.response.CommentDetailResponseDTO;
import com.example.demo.qnaapi.entity.QuestionBoard;
import com.example.demo.qnaapi.entity.QuestionComment;
import com.example.demo.qnaapi.service.QnaService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.validation.BindingResult;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@Slf4j
@RequiredArgsConstructor
@RequestMapping("/api/qna-board")
@CrossOrigin // 리액트와 연결하는 아노테이션 데이터를 브라우저에 보내기 성공
public class QnaController {

    private final QnaService qnaService;

    // 게시물 리스트
    @GetMapping
    public ResponseEntity<?> postlist(
    @AuthenticationPrincipal TokenUserInfo userInfo
    ){
        log.info("/api/qna-board GET request");
        List<BoardDetailResponseDTO> boardList = qnaService.getBoardList();
        return ResponseEntity.ok().body(boardList);
    }

    // 게시물 상세보기
    @GetMapping("/{id}")
    public ResponseEntity<?> postdetails(@PathVariable int id){
        log.info("/api/qna-board/{} GET - board Detail Request", id);
        BoardDetailResponseDTO getboard = qnaService.getboard(id);
        return ResponseEntity.ok().body(getboard);
    }

    // 게시물추가
    @PostMapping
    public ResponseEntity<?> postadd(
            @AuthenticationPrincipal TokenUserInfo userInfo,
            @Validated @RequestBody BoardCreateRequestDTO requestDTO, // 타이틀이 올 것이다 json의 형태로 @RequestBody
            BindingResult result // 요청 처리에 대한 결과를 담은 객체
    ){
        log.info("aaaaaaaaaaaaaaaaaaaaaaaaa{}", userInfo);
        log.info("/api/qna-board/{} POST - board Register Request", requestDTO);
        QuestionBoard addboard = qnaService.addboard(requestDTO, userInfo);

        return ResponseEntity.ok().body(addboard);

    }

    // 게시물수정
    @PutMapping("/{id}")
    public ResponseEntity<?> postupdate(
            @PathVariable int id,
            @AuthenticationPrincipal TokenUserInfo userInfo,
            @Validated @RequestBody BoardModifyRequestDTO requestDTO, // 타이틀이 올 것이다 json의 형태로 @RequestBody
            BindingResult result // 요청 처리에 대한 결과를 담은 객체
    ){

        log.info("/api/qna-board/{} POST - board Register Request", requestDTO);
        BoardDetailResponseDTO updateboard = qnaService.updateboard(requestDTO, id, userInfo);

        return ResponseEntity.ok().body(updateboard);

    }

    // 게시물삭제
    @DeleteMapping("/{id}")
    public ResponseEntity<?> postdelete(
            @AuthenticationPrincipal TokenUserInfo userInfo,
            @PathVariable("id") String id
            ){
        if(id == null || id.trim().equals("")) {
            ResponseEntity<BoardListResponseDTO> body = ResponseEntity
                    .badRequest()
                    .body(BoardListResponseDTO
                            .builder()
                            .error("ID를 전달해 주세요.")
                            .build());
            return body;
        }

        qnaService.deleteboard(Integer.parseInt(id), userInfo.getUserId());
        return null;
    }


    /************************** 댓글 요청 처리 ***************************/


    // 게시글 댓글 리스트
    @GetMapping("/{b_id}/reply")
    public List<?> commentlist(
            @PathVariable("b_id") int boardId

    ){
        List<CommentDetailResponseDTO> getboardcomment = qnaService.getboardcomment(boardId);

        log.info(";;;;;;;;;;;;;;;;;;;;;;;;;;;;{}", getboardcomment);
        return ResponseEntity.ok().body(getboardcomment).getBody();
    }

    // 댓글 추가
    @PostMapping("/{b_id}/reply")
    public ResponseEntity<?> commentadd(
            @AuthenticationPrincipal TokenUserInfo userInfo,
            @Validated @RequestBody CommentCreateRequestDTO requestDTO, // 타이틀이 올 것이다 json의 형태로 @RequestBody
            @PathVariable("b_id") int boardId,
            BindingResult result
    ) {
        log.info("aaaaaaaaaaaaaaaaaaaaaaaaa{}", userInfo);
        log.info("/api/qna-board/{} POST - board Register Request", requestDTO);
        CommentDetailResponseDTO addcomment = qnaService.addcomment(requestDTO, userInfo, boardId);

        return ResponseEntity.ok().body(addcomment);
    }

    // 댓글 수정
    @PutMapping("/{b_id}/reply/{r_id}")
    public ResponseEntity<?> commentupdate(
            @AuthenticationPrincipal TokenUserInfo userInfo,
            @Validated @RequestBody CommentModifyRequestDTO requestDTO,
            @PathVariable("b_id") int boardId,
            @PathVariable("r_id") int commentId,
            BindingResult result
    ){
        log.info("/api/qna-board/{}/reply/{}  - comment Update Request",
                boardId, commentId);
        log.info("modifying dto: {}, id: {}", requestDTO, commentId);

        try {
            CommentDetailResponseDTO commentDetailResponseDTO = qnaService.updateComment(requestDTO, userInfo.getUserId(), boardId, commentId);
            return ResponseEntity.ok().body(commentDetailResponseDTO);
        } catch (RuntimeException e) {
            return ResponseEntity
                    .internalServerError()
                    .body(e.getMessage());
        }

    }



    // 댓글 삭제
    @DeleteMapping("/{b_id}/reply/{r_id}")
    public void commentdelete(@AuthenticationPrincipal TokenUserInfo userInfo,
                              @PathVariable("b_id") int boardId,
                              @PathVariable("r_id") int commentId){
        qnaService.boardCommentDel(userInfo, boardId, commentId);

    }

}