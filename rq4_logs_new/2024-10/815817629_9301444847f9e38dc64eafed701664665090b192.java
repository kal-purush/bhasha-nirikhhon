package jpabook.jpashop.repository;

import com.querydsl.core.QueryResults;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jpabook.jpashop.domain.entity.Notice;
import jpabook.jpashop.domain.entity.QNotice;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ObjectUtils;

import java.util.List;

@SpringBootTest
@Transactional
class NoticeJpaRepositoryTest {

    @PersistenceContext
    private EntityManager em;

    @Autowired
    private NoticeJpaRepository noticeJpaRepository;

    private JPAQueryFactory jpaQueryFactory;

    QNotice notice;

    @BeforeEach
    void setUp() {
        jpaQueryFactory = new JPAQueryFactory(em);
        notice = QNotice.notice;
    }

    @Test
    @Commit
    void 공지사항_등록() {

        for (int i = 0; i<= 150; i++) {
            // 1. Notice 엔티티 생성
            Notice noticeEntity = Notice.builder()
                    .title("새 공지사항" + i)
                    .contents("공지사항 내용입니다." + i)
                    .build();

            // 2. EntityManager를 통해 실제로 DB에 삽입
            em.persist(noticeEntity);
        }

        QNotice notice = QNotice.notice;
        long count = jpaQueryFactory.selectFrom(notice)
                .fetchCount();

        Assertions.assertEquals(count, 152L);
    }

    @Test
    void 공지사항_삭제() {

        QNotice notice = QNotice.notice;
        Notice findByNoticeSeq = jpaQueryFactory.selectFrom(notice)
                .where(notice.seq.eq(1L))
                .fetchOne();

        Assertions.assertFalse(ObjectUtils.isEmpty(findByNoticeSeq));
        Assertions.assertEquals(findByNoticeSeq.getTitle(), "새 공지사항");

        em.remove(findByNoticeSeq);

        findByNoticeSeq = jpaQueryFactory.selectFrom(notice)
                .where(notice.seq.eq(1L))
                .fetchOne();

        Assertions.assertTrue(ObjectUtils.isEmpty(findByNoticeSeq));
    }

    @Test
    void 페이징_테스트() {

        List<Notice> findLimitNoticeList = jpaQueryFactory.selectFrom(notice)
                .orderBy(notice.seq.desc())
                .offset(0)
                .limit(2)
                .fetch();

        System.out.println("findLimitNoticeList = " + findLimitNoticeList);
    }

    @Test
    void 페이징_카운트_추가() {

        QueryResults<Notice> queryResults = jpaQueryFactory.selectFrom(notice)
                .orderBy(notice.seq.desc())
                .offset(0)
                .limit(2)
                .fetchResults();

        Assertions.assertEquals(queryResults.getTotal(), 152);
    }
}