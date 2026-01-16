package kr.ac.kumoh.illdang100.tovalley.service.comment;

import kr.ac.kumoh.illdang100.tovalley.domain.comment.Comment;
import kr.ac.kumoh.illdang100.tovalley.domain.comment.CommentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CommentServiceImpl implements CommentService {

    private final CommentRepository commentRepository;

    @Override
    public void deleteCommentByLostFoundBoardIdInBatch(Long lostFoundBoardId) {
        List<Comment> findCommentList = commentRepository.findCommentByLostFoundBoardId(lostFoundBoardId);

        if (!findCommentList.isEmpty()) {
            commentRepository.deleteAllByIdInBatch(findCommentList.stream()
                    .map(Comment::getId)
                    .collect(Collectors.toList()));
        }
    }
}