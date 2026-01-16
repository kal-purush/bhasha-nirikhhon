package com.numo.api.domain.wordbook.folder.repository.query;

import com.numo.api.domain.wordbook.folder.dto.FolderResponseDto;
import com.numo.api.domain.wordbook.folder.dto.read.FolderInWordCountDto;
import com.numo.domain.word.QWord;
import com.numo.domain.word.folder.QFolder;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.Wildcard;
import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
@RequiredArgsConstructor
public class FolderQueryRepository {
    private final JPAQueryFactory queryFactory;

    QFolder qFolder = QFolder.folder;
    QWord qWord = QWord.word1;

    /**
     * 모든 폴더의 안에 있는 단어 개수 조회
     *
     * @param userId 유저 아이디
     * @return 폴더 안에 있는 단어 개수
     */
    public List<FolderInWordCountDto> countWordInFolder(Long userId) {
        List<FolderInWordCountDto> result = queryFactory.select(Projections.constructor(
                        FolderInWordCountDto.class,
                        qWord.folder.folderId,
                        Expressions.as(Wildcard.count, "count")
                ))
                .from(qWord)
                .where(
                        qWord.folder.folderId.isNotNull(),
                        qWord.user.userId.eq(userId)
                )
                .groupBy(qWord.folder.folderId)
                .fetch();

        return result;
    }

    /**
     * 암기 여부를 포함한 해당 폴더의 단어 개수 조회
     *
     * @param userId 유저 아이디
     * @param folderId 폴더 아이디
     * @param memorization 암기 여부
     * @return 암기 여부를 포함한 해당 폴더의 단어 카운트 수
     */
    public Long countWordInFolderById(Long userId, Long folderId, Boolean memorization) {
        Long result = queryFactory.select(Expressions.as(Wildcard.count, "count"))
                .from(qWord)
                .where(
                        qWord.folder.folderId.eq(folderId),
                        qWord.user.userId.eq(userId),
                        eqMemorization(memorization)
                ).fetchOne();
        return result;
    }

    /**
     * 폴더를 조회한다.
     *
     * @param userId   조회할 유저 아이디(Nullable)
     * @param folderId 조회할 폴더 아이디(Nullable)
     * @return 조회한 폴더 데이터
     */
    public List<FolderResponseDto> getFoldersByUserId(Long userId, Long folderId) {
        List<FolderResponseDto> results = queryFactory.select(Projections.constructor(
                        FolderResponseDto.class,
                        qFolder.folderId,
                        qFolder.folderName,
                        qFolder.memo,
                        qFolder.color,
                        qFolder.background
                ))
                .from(qFolder)
                .where(
                        qFolder.user.userId.eq(userId),
                        eqFolderId(folderId)
                )
                .fetch();
        return results;
    }

    private BooleanExpression eqMemorization(Boolean memorization) {
        if (memorization == null) {
            return null;
        }
        String result = memorization? "Y" : "N";
        return qWord.memorization.eq(result);
    }

    private BooleanExpression eqFolderId(Long folderId) {
        if (folderId == null) {
            return null;
        }
        return qFolder.folderId.eq(folderId);
    }
}