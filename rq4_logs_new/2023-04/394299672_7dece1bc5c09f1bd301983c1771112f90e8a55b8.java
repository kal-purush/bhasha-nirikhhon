package uk.gov.hmcts.reform.laubackend.cases.repository.helpers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.jpa.convert.QueryByExamplePredicateBuilder;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import uk.gov.hmcts.reform.laubackend.cases.domain.CaseActionAudit;
import uk.gov.hmcts.reform.laubackend.cases.dto.ActionInputParamsHolder;
import uk.gov.hmcts.reform.laubackend.cases.utils.TimestampUtil;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.criteria.Predicate;

import static org.apache.commons.lang3.StringUtils.upperCase;

@Service
public class QueryBuilder {

    private static final String TIMESTAMP = "timestamp";
    @Autowired
    private TimestampUtil timestampUtil;


    public Specification<CaseActionAudit> buildCaseActionRequest(final ActionInputParamsHolder inputParamsHolder) {
        final CaseActionAudit caseActionAudit = createCaseActionAudit(inputParamsHolder);

        return getCaseActionAuditSpec(
                timestampUtil.getTimestampValue(inputParamsHolder.getStartTime()),
                timestampUtil.getTimestampValue(inputParamsHolder.getEndTime()),
                Example.of(caseActionAudit, getExampleMatcher()));
    }


    private Specification<CaseActionAudit> getCaseActionAuditSpec(final Timestamp startTime,
                                                                  final Timestamp endTime,
                                                                  final Example<CaseActionAudit> example) {
        return (root, query, builder) -> {
            final List<Predicate> predicates = new ArrayList<>();
            
            predicates.add(builder.greaterThanOrEqualTo(root.get(TIMESTAMP), startTime));
            predicates.add(builder.lessThanOrEqualTo(root.get(TIMESTAMP), endTime));
            
            predicates.add(QueryByExamplePredicateBuilder.getPredicate(root, builder, example));

            return builder.and(predicates.toArray(new Predicate[0]));
        };
    }

    private CaseActionAudit createCaseActionAudit(final ActionInputParamsHolder inputParamsHolder) {
        return new CaseActionAudit(
                inputParamsHolder.getUserId(),
                inputParamsHolder.getCaseRef(),
                upperCase(inputParamsHolder.getCaseAction()),
                upperCase(inputParamsHolder.getCaseJurisdictionId()),
                upperCase(inputParamsHolder.getCaseTypeId())
        );
    }

    private ExampleMatcher getExampleMatcher() {
        return ExampleMatcher.matching()
                .withIgnoreNullValues();
    }
}