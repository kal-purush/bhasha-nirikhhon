package com.rljj.chipservice.domain.chipexchange.service;

import com.rljj.chipservice.domain.chipexchange.dto.ChipExchangeCreateRequest;
import com.rljj.chipservice.domain.chipexchange.dto.ChipExchangeResponse;
import com.rljj.chipservice.domain.chippost.dto.ChipPostRequest;
import com.rljj.chipservice.domain.chippost.dto.ChipPostResponse;
import com.rljj.chipservice.domain.chippost.service.ChipPostService;
import com.rljj.switchswitchentity.chip.chipexchange.ChipExchangeStatus;
import com.rljj.switchswitchentity.chip.chippost.ChipPostStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.IteratorAssert.assertThatIterator;

@SpringBootTest
@Transactional
@EntityScan(basePackages = "com.rljj.switchswitchentity")
@Sql(statements = {
        "INSERT INTO member (id, email, nickname, password) VALUES (1, 'test@test.com', 'test1', 'test1');",
        "INSERT INTO chip_info (id, name, image_url, price, console_model) VALUES (1, 'test1', 'image url', '$100', 'NINTENDO_SWITCH1')"
})
public class ChipExchangeServiceTest {

    @Autowired
    private ChipExchangeService chipExchangeService;

    @Autowired
    private ChipPostService chipPostService;

    private User testUser;

    @BeforeEach
    void setUp() {
        initChipPost();
        initUser();
    }

    @Test
    void testChipPostExist() {
        ChipPostResponse post = chipPostService.getPost(1L);
        assertThat(post).isNotNull();
    }

    @Test
    void testChipExchangeSave() {
        // given
        ChipPostResponse post = chipPostService.getPost(1L);
        ChipExchangeCreateRequest request = getChipExchangeCreateRequest(post.getId(), post.getChipInfoId());

        // when
        chipExchangeService.createChipExchange(testUser, request);
        Page<ChipExchangeResponse> chipExchanges = chipExchangeService.getChipExchanges(post.getId(), Pageable.ofSize(10));

        // then
        assertThat(chipExchanges.getTotalElements()).isEqualTo(1);
        assertThatIterator(chipExchanges.iterator()).isNotNull();
    }

    private void initChipPost() {
        ChipPostRequest request = new ChipPostRequest(1L, 1L, "test post", "test desc", ChipPostStatus.OPEN);
        chipPostService.createPost(request);
    }

    private void initUser() {
        testUser = new User(String.valueOf(1), "test1", List.of(new SimpleGrantedAuthority("ROLE_USER")));
    }

    private ChipExchangeCreateRequest getChipExchangeCreateRequest(Long postId, Long chipInfoId) {
        return new ChipExchangeCreateRequest(postId, chipInfoId, "test", ChipExchangeStatus.REQUESTED);
    }
}