package com.challenger.fridge.repository;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;

import com.challenger.fridge.domain.Cart;
import com.challenger.fridge.domain.CartItem;
import com.challenger.fridge.domain.Member;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@Transactional
class CartRepositoryTest {

    @Autowired
    CartRepository cartRepository;
    @Autowired
    MemberRepository memberRepository;

    @DisplayName("장바구니 조회")
    @Test
    void findCart() {
        String email = "bbb@test.com";

        Cart cart = cartRepository.findByMemberEmail(email)
                .orElseThrow(IllegalArgumentException::new);
        Member member = cart.getMember();
        Member findMember = memberRepository.findByEmail(email)
                .orElseThrow(IllegalArgumentException::new);

        assertThat(member.getName()).isEqualTo(findMember.getName());
    }
}