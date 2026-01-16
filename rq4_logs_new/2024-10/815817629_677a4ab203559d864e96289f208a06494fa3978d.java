package jpabook.jpashop.repository;

import com.querydsl.core.Tuple;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import jpabook.jpashop.domain.entity.QQueryDslMember;
import jpabook.jpashop.domain.entity.QueryDslMember;
import org.springframework.stereotype.Repository;

import javax.swing.text.html.parser.Entity;
import java.util.List;
import java.util.Optional;

@Repository
public class MemberJpaQueryDslRepository {

    private final EntityManager em;
    private final JPAQueryFactory queryFactory;

    public MemberJpaQueryDslRepository(EntityManager em) {
        this.em = em;
        this.queryFactory = new JPAQueryFactory(em);
    }

    public void save(QueryDslMember queryDslMember) {
        em.persist(queryDslMember);
    }

    public Optional<QueryDslMember> findById(Long id) {
        QueryDslMember findByMember = em.find(QueryDslMember.class, id);

        return Optional.ofNullable(findByMember);
    }

    public List<QueryDslMember> findAll() {
        return em.createQuery("select m from QueryDslMember m", QueryDslMember.class)
                .getResultList();
    }

    public List<QueryDslMember> findAllForQueryDsl() {

        QQueryDslMember qQueryDslMember = QQueryDslMember.queryDslMember;

        return queryFactory
                .selectFrom(qQueryDslMember)
                .fetch();
    }

    public List<QueryDslMember> findByUserName(String userName) {
        return em.createQuery("select m from QueryDslMember m where m.userName = :username", QueryDslMember.class)
                .setParameter("username", userName)
                .getResultList();
    }

    public List<QueryDslMember> findByUserNameForQueryDsl(String userName) {
        QQueryDslMember m = QQueryDslMember.queryDslMember;

        return queryFactory.selectFrom(m)
                .where(m.userName.eq(userName))
                .fetch();
    }

}