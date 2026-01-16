package com.example.docconneting.domain.alarm.controller;

import com.example.docconneting.common.config.JwtUtil;
import com.example.docconneting.common.filter.JwtFilter;
import com.example.docconneting.common.resolver.AuthUserArgumentResolver;
import com.example.docconneting.common.response.PageInfo;
import com.example.docconneting.common.response.PageResult;
import com.example.docconneting.domain.alarm.dto.AlarmResponse;
import com.example.docconneting.domain.alarm.entity.AlarmHistories;
import com.example.docconneting.domain.alarm.enums.AlarmType;
import com.example.docconneting.domain.alarm.service.AlarmService;
import com.example.docconneting.domain.auth.entity.AuthUser;
import com.example.docconneting.domain.user.enums.UserRole;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.mapping.JpaMetamodelMappingContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

@WebMvcTest(AlarmController.class)
@TestPropertySource(properties = {
        "jwt.secret.key=5Gk6hibHDtKLFVk4NdBX039rvehSLNjfKsdXpm/pHsU="
})
@Import({JwtUtil.class, AuthUserArgumentResolver.class, JwtFilter.class})
class AlarmControllerTest {

    @Autowired
    MockMvc mockMvc;

    @MockitoBean
    JpaMetamodelMappingContext jpaMetamodelMappingContext;

    @MockitoBean
    AlarmService alarmService;

    @Autowired
    JwtUtil jwtUtil;

    AlarmHistories histories1;
    AlarmHistories histories2;

    @BeforeEach
    void setUp() {
        histories1 = AlarmHistories.of(
                "회원님의 글에 댓글이 달렸습니다",
                1L,
                AlarmType.COMMENT
        );
        ReflectionTestUtils.setField(histories1, "id", 1L);
        ReflectionTestUtils.setField(histories1, "createdAt", LocalDateTime.now());

        histories2 = AlarmHistories.of(
                "회원님의 글에 댓글이 달렸습니다",
                1L,
                AlarmType.COMMENT
        );
        ReflectionTestUtils.setField(histories2, "id", 2L);
        ReflectionTestUtils.setField(histories2, "createdAt", LocalDateTime.now());
    }

    @Test
    public void 알람_내역을_페이징을_이용하여_조회한다() throws Exception {
        // given
        int page = 0;
        int size = 10;
        int totalElement = 2;
        int totalPages = 1;

        Pageable pageable = PageRequest.of(page,size);

        Long userId = 1L;
        UserRole userRole = UserRole.PATIENT;
        AuthUser authUser = AuthUser.of(userId, userRole);

        String accessToken = jwtUtil.createToken(userId, userRole);

        List<AlarmHistories> alarmHistories = new ArrayList<>();
        alarmHistories.add(histories1);
        alarmHistories.add(histories2);
        List<AlarmResponse> alarmResponses = AlarmResponse.toAlarmResponse(alarmHistories);

        PageInfo pageInfo = PageInfo.builder()
                .pageNum(page)
                .pageSize(size)
                .totalElement(totalElement)
                .totalPage(totalPages)
                .build();

        PageResult<AlarmResponse> pageResult = new PageResult<>(alarmResponses, pageInfo);

        given(alarmService.findAlarms(refEq(authUser)
                , argThat(p -> p.getPageNumber() == pageable.getPageNumber() && p.getPageSize() == pageable.getPageSize())))
                .willReturn(pageResult);

        // when & then
        mockMvc.perform(get("/api/v1/notifications")
                        .param("page", "0")
                        .param("size", "10")
                        .header("Authorization", accessToken))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[0].id").value(histories1.getId()))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[0].content").value(histories1.getContent()))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[0].alarmType").value(histories1.getAlarmType().name()))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[1].id").value(histories2.getId()))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[1].content").value(histories2.getContent()))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[1].alarmType").value(histories2.getAlarmType().name()))
                .andExpect(MockMvcResultMatchers.jsonPath("$.page.pageNum").value(page))
                .andExpect(MockMvcResultMatchers.jsonPath("$.page.pageSize").value(size))
                .andExpect(MockMvcResultMatchers.jsonPath("$.page.totalElement").value(totalElement))
                .andExpect(MockMvcResultMatchers.jsonPath("$.page.totalPage").value(totalPages));
    }

}