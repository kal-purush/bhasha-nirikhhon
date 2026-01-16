package com.petid.infra.member.repository;

import com.petid.domain.member.model.Member;
import com.petid.infra.member.entity.QMemberEntity;
import com.petid.infra.pet.entity.QPetEntity;
import com.querydsl.core.types.Projections;
import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.List;



@Repository
@RequiredArgsConstructor
public class QMemberRepository {

    private final JPAQueryFactory queryFactory;

    public List<Member> findMembersWithoutPets() {
       
    	QMemberEntity memberEntity =   QMemberEntity.memberEntity;
        QPetEntity petEntity = QPetEntity.petEntity;
        
        return queryFactory
            .select( 
            		Projections.constructor(Member.class,
            				 memberEntity.id,
            				 memberEntity.uid,
            				 memberEntity.platform,
            				 memberEntity.fcmToken,
            				 memberEntity.email,
            				 memberEntity.role
            				 )           				
            		)
            .from(memberEntity)
            .leftJoin(petEntity).on(petEntity.ownerId.eq(memberEntity.id))
            .where(petEntity.id.isNull())
            .fetch();      
        
    }
}