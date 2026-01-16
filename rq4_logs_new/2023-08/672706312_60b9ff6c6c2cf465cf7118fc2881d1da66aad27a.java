package com.openpromt.coffeee.swf2023.openpromtserver.ownticket.repository;

import com.openpromt.coffeee.swf2023.openpromtserver.ownticket.entity.OwnTicket;
import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.List;

import static com.openpromt.coffeee.swf2023.openpromtserver.ownticket.entity.QOwnTicket.ownTicket;

@RequiredArgsConstructor
@Repository
public class QueryDslOwnTicketRepositoryImpl implements QueryDslOwnTicketRepository{

    private final JPAQueryFactory jpaQueryFactory;

    @Override
    public List<OwnTicket> listByUsername(String username) {

        return jpaQueryFactory
                .selectFrom(ownTicket)
                .where(ownTicket.userId.username.eq(username))
                .fetch();
    }
}