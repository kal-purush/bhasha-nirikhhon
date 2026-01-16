package com.spartanullnull.otil.global.aspect;

import com.spartanullnull.otil.domain.comment.entity.*;
import com.spartanullnull.otil.domain.comment.exception.*;
import com.spartanullnull.otil.domain.comment.repository.*;
import com.spartanullnull.otil.domain.post.repository.*;
import com.spartanullnull.otil.domain.user.entity.*;
import com.spartanullnull.otil.domain.user.repository.*;
import com.spartanullnull.otil.security.Impl.*;
import java.util.*;
import lombok.*;
import lombok.extern.slf4j.*;
import org.aspectj.lang.annotation.*;

@Slf4j(topic = "Check Resource's Author")
@Aspect
@RequiredArgsConstructor
public class AuthorValidationAspect {

    private final UserRepository userRepository;
    private final CommentRepository commentRepository;
    private final PostRepository postRepository;

//     비밀번호 수정 :: modifyPassword
//     프로필 수정 :: modifyUser
//     아직 merge가 안 되어서 메서드명을 추측해봄
//    @Pointcut(
//        "execution(* com.spartanullnull.otil.domain.user.controller.UserController.modifyPassword()) || "
//            + "execution(* com.spartanullnull.otil.domain.user.controller.UserController.modifyUser())")
//    private void validateUserPointCut() {
//    }

//     게시글 수정 :: modifyPost
//     게시글 삭제 :: deletePost
//     아직 merge가 안 되어서 메서드명을 추측해봄
//    @Pointcut(
//        "execution(* com.spartanullnull.otil.domain.post.controller.PostController.modifyPost()) || "
//            + "execution(* com.spartanullnull.otil.domain.post.controller.PostController.deletePost())")
//    private void validatePostPointCut() {
//    }

    // 댓글 수정
    // 댓글 삭제
    @Pointcut(
        "execution(* com.spartanullnull.otil.domain.comment.controller.CommentController.updateComment()) || "
            + "execution(* com.spartanullnull.otil.domain.comment.controller.CommentController.deleteComment())")
    private void validateCommentPointCut() {
    }

    @Before(value = "validateCommentPointCut() && args(postId,commentId,userDetails,..)", argNames = "postId,commentId,userDetails")
    public void validateOwnerOfComment(Long postId, Long commentId, UserDetailsImpl userDetails)
        throws Throwable {
        User user = userDetails.getUser();
        List<Comment> foundComments = commentRepository.findByIdAndPostIdAndUser(postId, commentId,
            user);
        if (foundComments.isEmpty()) {
            throw new NotAuthorOfCommentException("commentId", commentId.toString(),
                "해당 댓글에 대한 소유주가 아닙니다.");
        }
    }
}