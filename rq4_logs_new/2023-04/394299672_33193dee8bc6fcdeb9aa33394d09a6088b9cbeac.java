package uk.gov.hmcts.reform.laubackend.cases.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import uk.gov.hmcts.reform.laubackend.cases.domain.CaseSearchAudit;

import java.sql.Timestamp;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;

import static org.apache.commons.lang3.StringUtils.isEmpty;

@SuppressWarnings("unchecked")
@Repository
public class CaseSearchAuditFindCaseRepository {
    private static final String SELECT_CRITERIA = "SELECT csa.id, csa.user_id, csa.log_timestamp, csa.case_refs";
    private static final String CASE_REF_AND_CRITERIA = " AND csa.case_refs @> ARRAY[cast(:caseRef as bigint)]";
    private static final String USER_ID_AND_CRITERIA = " AND csa.user_id=:userId";
    private static final String FROM_CRITERIA = " FROM case_search_audit csa where "
            + "csa.log_timestamp>=:startTime AND csa.log_timestamp<=:endTime";

    @PersistenceContext
    private final EntityManager entityManager;

    @Autowired
    public CaseSearchAuditFindCaseRepository(final EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    @Transactional(readOnly = true)
    public Page<CaseSearchAudit> findCaseSearch(final String userId,
                                                final String caseRef,
                                                final Timestamp startTime,
                                                final Timestamp endTime,
                                                final Pageable pageable) {
        final StringBuilder queryString = new StringBuilder(SELECT_CRITERIA + FROM_CRITERIA);

        addSearchCriteria(queryString, userId, caseRef);

        final Query query = entityManager.createNativeQuery(queryString.toString(), CaseSearchAudit.class);

        setSearchParams(query, startTime, endTime, userId, caseRef);

        query.setFirstResult((int) pageable.getOffset());
        query.setMaxResults(pageable.getPageSize());

        final List<CaseSearchAudit> results = (List<CaseSearchAudit>) query.getResultList();
        long totalCount = countResults(userId, caseRef, startTime, endTime);

        return new PageImpl<>(results, pageable, totalCount);
    }

    private long countResults(final String userId,
                              final String caseRef,
                              final Timestamp startTime,
                              final Timestamp endTime) {

        final StringBuilder queryString = new StringBuilder("SELECT 1 " + FROM_CRITERIA);

        addSearchCriteria(queryString, userId, caseRef);

        final String sqlCountString = "SELECT count(*) FROM ("
                .concat(queryString.toString())
                .concat(" LIMIT 10000) cd");

        final Query countQuery = entityManager.createNativeQuery(sqlCountString);

        setSearchParams(countQuery, startTime, endTime, userId, caseRef);

        return ((Number) countQuery.getSingleResult()).intValue();
    }

    private void addSearchCriteria(final StringBuilder queryString,
                                   final String userId,
                                   final String caseRef) {
        if (!isEmpty(userId)) {
            queryString.append(USER_ID_AND_CRITERIA);
        }
        if (!isEmpty(caseRef)) {
            queryString.append(CASE_REF_AND_CRITERIA);
        }
    }

    private void setSearchParams(final Query query, final Timestamp startTime, final Timestamp endTime,
                                 final String userId,
                                 final String caseRef) {
        query.setParameter("startTime", startTime);
        query.setParameter("endTime", endTime);

        if (!isEmpty(userId)) {
            query.setParameter("userId", userId);
        }
        if (!isEmpty(caseRef)) {
            query.setParameter("caseRef", caseRef);
        }
    }
}