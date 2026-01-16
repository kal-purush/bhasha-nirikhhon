package jpabook.jpashop;

import io.micrometer.common.util.StringUtils;
import jpabook.jpashop.domain.entity.JpaMember;
import jpabook.jpashop.repository.MemberJpaRepository;
import jpabook.jpashop.service.MemberService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ObjectUtils;

import java.util.Optional;

@SpringBootTest
@Transactional
public class MockTest {

    @Mock
    private MemberJpaRepository memberMockJpaRepository;

    @InjectMocks
    private MemberService memberInjectMocksService;

    @MockBean
    private MemberJpaRepository memberJpaRepository;

    @Test
    void injectMocksTest() {

        // given
        JpaMember member = new JpaMember("user41");
        Mockito.when(memberMockJpaRepository.findByUserName("user41")).thenReturn(member);

        // when
        JpaMember findMemberInfo = memberInjectMocksService.findMemberByUserName("user41");

        Assertions.assertEquals(findMemberInfo.getUserName(), member.getUserName());
    }




    @Test
    void mockSuccessTest() {

        // given
        JpaMember member = new JpaMember("user43");
        Mockito.when(memberJpaRepository.findByUserName("user43")).thenReturn(member);

        // when
        JpaMember findMemberInfo = memberJpaRepository.findByUserName("user43");

        Assertions.assertEquals(findMemberInfo.getUserName(), member.getUserName());
    }

    @Test
    void mockFailTest() {

        // given
        JpaMember member = new JpaMember("user43");
        Mockito.when(memberJpaRepository.findByUserName("user42")).thenReturn(member);

        // when
        JpaMember findMemberInfo = memberJpaRepository.findByUserName("user43");

        Assertions.assertTrue(ObjectUtils.isEmpty(findMemberInfo));
    }


}