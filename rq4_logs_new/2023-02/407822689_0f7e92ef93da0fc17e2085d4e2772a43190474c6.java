package com.fly.jpa;

import com.fly.jpa.onetoone2.infrastructure.entity.AddressTwoEntity;
import com.fly.jpa.onetoone2.infrastructure.entity.UsersTwoEntity;
import com.fly.jpa.onetoone2.infrastructure.repository.jpa.AddressTwoJpaRepository;
import com.fly.jpa.onetoone2.infrastructure.repository.jpa.UsersTwoJpaRepository;
import jakarta.persistence.EntityManager;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import static org.hamcrest.MatcherAssert.assertThat;

class UserTwoTest extends BaseIsolationTest {
    @Autowired
    private UsersTwoJpaRepository usersTwoJpaRepository;

    @Autowired
    private AddressTwoJpaRepository addressTwoJpaRepository;

    @Autowired
    private EntityManager entityManager;

    @Nested
    class TestSameTransaction1 {

        void before() {
            var usersTwoEntity = new UsersTwoEntity();
            usersTwoEntity.setId(3L);
            usersTwoEntity.setUsername("ceshi");
            var addressTwoEntity = new AddressTwoEntity();
            addressTwoEntity.setUserId(3L);
            addressTwoEntity.setId(3L);
            addressTwoEntity.setCity("ceshi");
            //when
            usersTwoJpaRepository.saveAndFlush(usersTwoEntity);
            addressTwoJpaRepository.saveAndFlush(addressTwoEntity);
        }

        @Test
        void test_same_transaction2() {
            /**
             * 同一个事务不清理 jpa session 缓存
             */
            before();
            //then
            entityManager.clear();
            var byId = usersTwoJpaRepository.findById(3L).get();
            assertThat(byId, Matchers.notNullValue());
            assertThat(byId.getAddressTwo(), Matchers.notNullValue());
        }
    }
}