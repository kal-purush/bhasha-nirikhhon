package com.spartanullnull.otil.comment.service;

import com.spartanullnull.otil.comment.dto.CommentRequestDto;
import com.spartanullnull.otil.comment.dto.CommentResponseDto;
import com.spartanullnull.otil.comment.entity.Comment;
import com.spartanullnull.otil.comment.repository.CommentRepository;
import com.spartanullnull.otil.post.entity.Post;
import com.spartanullnull.otil.user.entity.User;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class CommentService {

    private final CommentRepository commentRepository;
    private final UserRepository userRepository;
    private final PostRepository postRepository;

    @Autowired
    public CommentService(CommentRepository commentRepository, UserRepository userRepository, PostRepository postRepository) {
        this.commentRepository = commentRepository;
        this.userRepository = userRepository;
        this.postRepository = postRepository;
    }

    // postId       작성된 댓글이 속한 TIL의 Id
    // userId       댓글을 작성한 사용자의 Id
    // requestDto   댓글 작성 요청 Dto
    // return       작성된 댓글의 응답 Dto

    // 댓글 작성 기능
    @Transactional
    public CommentResponseDto createComment(Long postId, Long userId, CommentRequestDTO requestDTO) {
        // 댓글 작성자 및 게시물 확인
        User user = userRepository.findById(userId)
            .orElseThrow(() -> new RuntimeException("사용자를 찾을 수 없습니다."));
        Post post = postRepository.findById(postId)
            .orElseThrow(() -> new RuntimeException("TIL을 찾을 수 없습니다."));

        // 새로운 댓글 생성
        Comment comment = new Comment();
        comment.setCommentText(requestDTO.getCommentText());
        comment.setUser(user);
        comment.setPost(post);

        Comment savedComment = commentRepository.save(comment);

        return CommentResponseDto.fromEntity(savedComment);
    }

    // PostId       조회할 댓글이 속한 TIL의 Id
    // return       댓글 목록의 응답 Dto 리스트
    // 특정 게시글에 대한 댓글 목록 조회 기능
    public List<CommentResponseDto> getCommentsByPostId(Long postId) {
        // 특정 게시물에 대한 댓글 목록 조회
        List<Comment> comments = commentRepository.findByPostId(postId);

        // 댓글 목록을 Dto로 변환하여 반환
        return comments.stream()
            .map(CommentResponseDto::fromEntity)
            .collect(Collectors.toList());
    }

    // commentId        수정할 댓글의 Id
    // requestDto       수정할 댓글의 내용이 담긴 Dto
    // return           수정된 댓글의 응답 Dto

    // 댓글 수정 기능
    public CommentResponseDto updateComment(Long commentId, CommentRequestDto requestDto) {
        // 기존 댓글 확인
        Comment existingComment = commentRepository.findById(commentId)
            .orElseThrow(() -> new RuntimeException("댓글을 찾을 수 없습니다."));

        // 댓글 내용 업데이트
        existingComment.setCommentText(requestDto.getCommentText());

        return CommentResponseDto.fromEntity(existingComment);
    }

    // comment Id       삭제할 댓글의 Id

    // 댓글 삭제 기능
    public void deleteComment(Long commentId) {
        // 댓글 삭제
        commentRepository.deleteById(commentId);
    }
}