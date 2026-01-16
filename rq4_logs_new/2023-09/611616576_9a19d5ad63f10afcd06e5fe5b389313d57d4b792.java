package com.seproject.board.post.persistence;

import com.querydsl.jpa.impl.JPAQueryFactory;
import com.seproject.board.post.domain.model.Post;
import com.seproject.file.domain.model.FileMetaData;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Set;

import static com.seproject.board.menu.domain.QCategory.category;
import static com.seproject.board.post.domain.model.QPost.post;
import static com.seproject.member.domain.QBoardUser.boardUser;

@Repository
@RequiredArgsConstructor
public class PostQueryRepository {

    private final JPAQueryFactory jpaQueryFactory;

    public Post findByIdWithAll(Long id) {

        Post findPost = jpaQueryFactory.select(post)
                .from(post)
                .leftJoin(post.category, category).fetchJoin()
                .leftJoin(post.author, boardUser).fetchJoin()
                .where(post.postId.eq(id))
                .fetchOne();

        Set<FileMetaData> attachments = findPost.getAttachments();
        return findPost;
    }

}