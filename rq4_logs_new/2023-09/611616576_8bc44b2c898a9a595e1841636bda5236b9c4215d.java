package com.seproject.board.post.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.seproject.account.account.domain.Account;
import com.seproject.account.account.domain.FormAccount;
import com.seproject.admin.domain.SelectOption;
import com.seproject.admin.menu.application.AdminMenuAppService;
import com.seproject.admin.menu.controller.dto.MenuDTO;
import com.seproject.admin.menu.utils.MenuRequestBuilder;
import com.seproject.board.menu.domain.Category;
import com.seproject.board.post.service.PostService;
import com.seproject.global.*;
import com.seproject.member.domain.Member;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

@SpringBootTest
@Transactional
@AutoConfigureMockMvc
class PostControllerTest {

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper objectMapper;
    @Autowired EntityManager em;

    @Autowired PostService postService;

    @Autowired PostSetup postSetup;
    @Autowired MenuSetup menuSetup;
    @Autowired BoardUserSetup boardUserSetup;
    @Autowired AccountSetup accountSetup;
    @Autowired TokenSetup tokenSetup;
    @Autowired RoleSetup roleSetup;
    @Autowired
    AdminMenuAppService adminMenuAppService;

    static final String url = "/posts/";
    
    @Test
    public void 게시글_목록_조회() throws Exception {

        Category category = menuSetup.createCategory(menuSetup.createBoardMenu(menuSetup.createMenu()));
        FormAccount formAccount = accountSetup.createFormAccount();
        Member member = boardUserSetup.createMember(formAccount);

        for (int i = 0; i < 43; i++) {
            postSetup.createPost(member, category);
        }

        em.flush(); em.clear();

        mvc.perform(MockMvcRequestBuilders.get(url)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .characterEncoding("UTF-8")
                .queryParam("categoryId", String.valueOf(category.getMenuId()))
                .queryParam("page" , "0")
                .queryParam("perPage", "50")
        ).andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(jsonPath("$.content.size()").value(43))
                .andExpect(jsonPath("$.empty").value(false))
                .andExpect(jsonPath("$.first").value(true))
                .andExpect(jsonPath("$.last").value(true))
                .andExpect(jsonPath("$.number").value(0))
                .andExpect(jsonPath("$.totalElements").value(43))
                .andExpect(jsonPath("$.numberOfElements").value(43))
                .andExpect(jsonPath("$.size").value(50));
    }

    @Test
    public void 게시글_카테고리_비로그인_권한_없음() throws Exception {
        Category category = menuSetup.createCategory(menuSetup.createBoardMenu(menuSetup.createMenu()));

        MenuDTO.MenuAuthOption edit = new MenuDTO.MenuAuthOption();
        edit.setName(SelectOption.SELECT.getName());
        edit.setRoles(List.of(roleSetup.createRole().getId()));

        MenuDTO.MenuAuthOption[] input = new MenuDTO.MenuAuthOption[] {null,null,edit,edit};
        MenuDTO.UpdateMenuRequest req = MenuRequestBuilder.getUpdateMenuRequest(input);

        Account adminAccount = accountSetup.getAdminAccount();

        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken(adminAccount,
                        null,adminAccount.getAuthorities()));

        adminMenuAppService.update(category.getMenuId(),req);

        em.flush(); em.clear();

        mvc.perform(MockMvcRequestBuilders.get(url)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .characterEncoding("UTF-8")
                .queryParam("categoryId", String.valueOf(category.getMenuId()))
                .queryParam("page" , "0")
                .queryParam("perPage", "50")
        ).andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isForbidden());
    }

    @Test
    public void 게시글_카테고리_로그인_권한_없음() throws Exception {
        Category category = menuSetup.createCategory(menuSetup.createBoardMenu(menuSetup.createMenu()));

        MenuDTO.MenuAuthOption edit = new MenuDTO.MenuAuthOption();
        edit.setName(SelectOption.SELECT.getName());
        edit.setRoles(List.of(roleSetup.createRole().getId()));

        MenuDTO.MenuAuthOption[] input = new MenuDTO.MenuAuthOption[] {null,null,edit,edit};
        MenuDTO.UpdateMenuRequest req = MenuRequestBuilder.getUpdateMenuRequest(input);

        Account adminAccount = accountSetup.getAdminAccount();

        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken(adminAccount,
                        null,adminAccount.getAuthorities()));

        adminMenuAppService.update(category.getMenuId(),req);

        em.flush(); em.clear();

        mvc.perform(MockMvcRequestBuilders.get(url)
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON)
                        .characterEncoding("UTF-8")
                        .queryParam("categoryId", String.valueOf(category.getMenuId()))
                        .queryParam("page" , "0")
                        .queryParam("perPage", "50")
                        .header("Authorization",tokenSetup.getAccessToken(adminAccount))
                ).andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isForbidden());
    }

    
    
    
    
    
    
    
    
    
    
    
}