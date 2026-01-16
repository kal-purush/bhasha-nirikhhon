package jaeseok.numble.mybox.member.repository;

import com.querydsl.core.types.Projections;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jaeseok.numble.mybox.member.dto.MemberInfoAndUsage;
import lombok.RequiredArgsConstructor;

import java.util.Optional;

import static jaeseok.numble.mybox.member.domain.QMember.member;

@RequiredArgsConstructor
public class MemberRepositoryImpl implements MemberRepositoryCustom{

    private final JPAQueryFactory queryFactory;

    @Override
    public Optional<MemberInfoAndUsage> findMemberAndUsageById(Long id) {
        return Optional.of(queryFactory
                .select(Projections.bean(MemberInfoAndUsage.class,
                        member.id,
                        member.email,
                        member.files.size().sum().longValue().as("byteUsage")))
                .from(member)
                .leftJoin(member.files).fetchJoin()
                .where(member.eq(member.files.any().owner)
                        .and(member.id.eq(id)))
                .groupBy(member.id, member.email)
                .fetch().get(0));
    }
}