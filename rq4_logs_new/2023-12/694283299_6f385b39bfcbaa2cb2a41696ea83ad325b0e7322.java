package com.myproject.bikereviewapp.controller;

import com.myproject.bikereviewapp.entity.Role;
import com.myproject.bikereviewapp.entity.User;
import com.myproject.bikereviewapp.service.abstraction.ReviewService;
import com.myproject.bikereviewapp.service.abstraction.UserService;
import com.myproject.bikereviewapp.utility.SortUtility;
import com.myproject.bikereviewapp.validation.validator.UserUniquenessValidator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.view;

@WebMvcTest(UserController.class)
@AutoConfigureMockMvc(addFilters = false)
class UserControllerTest {

    @MockBean
    UserService userService;

    @MockBean
    ReviewService reviewService;

    @MockBean
    UserUniquenessValidator uniquenessValidator;

    @MockBean
    SortUtility sortUtility;

    @Autowired
    MockMvc mockMvc;


    private static Page<User> userPage;
    private static int pageNumber;
    private static int pageSize;
    private static Sort sort;
    private static String sortStr;


    @BeforeAll
    static void init() {
        userPage = new PageImpl<>(List.of(
                new User(1L, "username1", "password1", true, Role.CLIENT, "publicName1"),
                new User(2L, "username2", "password2", true, Role.CLIENT, "publicName2"),
                new User(3L, "username3", "password3", true, Role.CLIENT, "publicName3")
                ));

        pageNumber = 5;

        pageSize = 12;

        sort = Sort.by(Sort.Direction.ASC, "id");

        sortStr = "id:asc";
    }


    @Test
    void showAllInAdminPanel_shouldReturnAppropriateView() throws Exception {

        when(sortUtility.parseSort(anyString())).thenReturn(sort);
        when(userService.getAll(any(PageRequest.class))).thenReturn(userPage);

        mockMvc.perform(get("/users/admin"))
                .andExpect(status().isOk())
                .andExpect(view().name("user/admin/all"));
    }

    @Test
    void showAllInAdminPanel_shouldAddUserPageModelAttribute() throws Exception {

        when(sortUtility.parseSort(anyString())).thenReturn(sort);
        when(userService.getAll(any(PageRequest.class))).thenReturn(userPage);

        mockMvc.perform(get("/users/admin"))
                .andExpect(status().isOk())
                .andExpect(model().attribute("userPage", userPage));
    }

    @Test
    void showAllInAdminPanel_shouldMakePageRequestByProperParams_whenRequestContainAppropriateParams() throws Exception {

        when(sortUtility.parseSort(sortStr)).thenReturn(sort);
        when(userService.getAll(any(PageRequest.class))).thenReturn(userPage);


        mockMvc.perform(get("/users/admin")
                        .param("pageNumber", String.valueOf(pageNumber))
                        .param("pageSize", String.valueOf(pageSize))
                        .param("sort", sortStr))
                .andExpect(status().isOk());

        verify(sortUtility).parseSort(sortStr);
        verify(userService).getAll(PageRequest.of(pageNumber, pageSize, sort));
    }

    @Test
    void showAllInAdminPanel_shouldAddPageNumberAndSortModelAttributes_whenRequestContainAppropriateParams() throws Exception {

        when(sortUtility.parseSort(anyString())).thenReturn(sort);
        when(userService.getAll(any(PageRequest.class))).thenReturn(userPage);


        mockMvc.perform(get("/users/admin")
                        .param("pageNumber", String.valueOf(pageNumber))
                        .param("pageSize", String.valueOf(pageSize))
                        .param("sort", sortStr))
                .andExpect(status().isOk())
                .andExpect(model().attribute("currentPageNumber", pageNumber))
                .andExpect(model().attribute("currentSort", sortStr));
    }

    @Test
    void showAllInAdminPanel_shouldAddDefaultPageNumberAndSortModelAttributes_whenRequestDoesNotContainAppropriateParams() throws Exception {

        when(sortUtility.parseSort(anyString())).thenReturn(sort);
        when(userService.getAll(any(PageRequest.class))).thenReturn(userPage);

        mockMvc.perform(get("/users/admin"))
                .andExpect(status().isOk())
                .andExpect(model().attributeExists("currentPageNumber"))
                .andExpect(model().attributeExists("currentSort"));
    }

}