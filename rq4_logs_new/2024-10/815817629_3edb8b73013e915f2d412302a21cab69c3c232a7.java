package jpabook.jpashop.repository;


import jpabook.jpashop.domain.entity.QueryDslMember;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface MemberJpaQueryDslRepository2 extends JpaRepository<QueryDslMember, Long>, memberJpaQueryDslCustomRepository{

    List<QueryDslMember> findByUserName(String userName);


}