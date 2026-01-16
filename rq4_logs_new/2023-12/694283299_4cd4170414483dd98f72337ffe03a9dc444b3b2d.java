package com.myproject.bikereviewapp.controller;

import com.myproject.bikereviewapp.entity.Brand;
import com.myproject.bikereviewapp.entity.Motorcycle;
import com.myproject.bikereviewapp.service.abstraction.BrandService;
import com.myproject.bikereviewapp.service.abstraction.MotorcycleService;
import com.myproject.bikereviewapp.service.abstraction.ReviewService;
import com.myproject.bikereviewapp.utility.SortUtility;
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

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.model;

@WebMvcTest(MotorcycleController.class)
@AutoConfigureMockMvc(addFilters = false)
class MotorcycleControllerTest {

    @MockBean
    MotorcycleService motorcycleService;

    @MockBean
    BrandService brandService;

    @MockBean
    ReviewService reviewService;

    @MockBean
    SortUtility sortUtility;

    @Autowired
    MockMvc mockMvc;

    @Autowired
    MotorcycleController motorcycleController;

    @Test
    void showAll_shouldReturnAppropriateView() throws Exception {

        Sort sampleSort = Sort.by(Sort.Direction.ASC, "model");
        Page<Motorcycle> motorcyclePage = Page.empty();

        when(sortUtility.parseSort(anyString())).thenReturn(sampleSort);
        when(motorcycleService.getAll(any(PageRequest.class))).thenReturn(motorcyclePage);

        mockMvc.perform(get("/motorcycles"))
                .andExpect(status().isOk())
                .andExpect(view().name("motorcycle/all"));
    }

    @Test
    void showAll_shouldAddMotorcyclePageModelAttribute() throws Exception {

        Sort sampleSort = Sort.by(Sort.Direction.ASC, "model");

        Page<Motorcycle> motorcyclePage = new PageImpl<>(Arrays.asList(
                new Motorcycle(1L, "model1", new Brand()),
                new Motorcycle(2L, "model2", new Brand()),
                new Motorcycle(3L, "model3", new Brand())
        ));

        when(sortUtility.parseSort(anyString())).thenReturn(sampleSort);
        when(motorcycleService.getAll(any(PageRequest.class))).thenReturn(motorcyclePage);

        mockMvc.perform(get("/motorcycles"))
                .andExpect(status().isOk())
                .andExpect(model().attribute("motorcyclePage", motorcyclePage));
    }

    @Test
    void showAll_shouldMakePageRequestByProperPageNumberAndSort_whenRequestContainAppropriateParams() throws Exception {
        int pageNumber = 2;
        int pageSize = 20;
        String sortStr = "model:asc";
        Sort sort = Sort.by(Sort.Direction.ASC, "model");

        Page<Motorcycle> motorcyclePage = Page.empty();

        when(sortUtility.parseSort(sortStr)).thenReturn(sort);
        when(motorcycleService.getAll(any(PageRequest.class))).thenReturn(motorcyclePage);


        mockMvc.perform(get("/motorcycles")
                        .param("pageNumber", String.valueOf(pageNumber))
                        .param("pageSize", String.valueOf(pageSize))
                        .param("sort", sortStr))
                .andExpect(status().isOk());

        verify(sortUtility).parseSort(sortStr);
        verify(motorcycleService).getAll(PageRequest.of(pageNumber, pageSize, sort));
    }

    @Test
    void showAll_shouldAddPageNumberAndSortModelAttributes_whenRequestContainAppropriateParams() throws Exception {
        int pageNumber = 2;
        int pageSize = 20;
        String sortStr = "name:asc";
        Sort sampleSort = Sort.by(Sort.Direction.ASC, "model");

        Page<Motorcycle> motorcyclePage = Page.empty();

        when(sortUtility.parseSort(anyString())).thenReturn(sampleSort);
        when(motorcycleService.getAll(any(PageRequest.class))).thenReturn(motorcyclePage);


        mockMvc.perform(get("/motorcycles")
                        .param("pageNumber", String.valueOf(pageNumber))
                        .param("pageSize", String.valueOf(pageSize))
                        .param("sort", sortStr))
                .andExpect(status().isOk())
                .andExpect(model().attribute("currentPageNumber", pageNumber))
                .andExpect(model().attribute("currentSort", sortStr));
    }

    @Test
    void showAll_shouldAddDefaultPageNumberAndSortModelAttributes_whenRequestDoesNotContainAppropriateParams() throws Exception {

        Sort sampleSort = Sort.by(Sort.Direction.ASC, "model");
        Page<Motorcycle> motorcyclePage = Page.empty();

        when(sortUtility.parseSort(anyString())).thenReturn(sampleSort);
        when(motorcycleService.getAll(any(PageRequest.class))).thenReturn(motorcyclePage);

        mockMvc.perform(get("/motorcycles"))
                .andExpect(status().isOk())
                .andExpect(model().attributeExists("currentPageNumber"))
                .andExpect(model().attributeExists("currentSort"));
    }

    @Test
    void showAllInAdminPanel_shouldReturnAppropriateView() throws Exception {

        Sort sampleSort = Sort.by(Sort.Direction.ASC, "model");
        Page<Motorcycle> motorcyclePage = Page.empty();

        when(sortUtility.parseSort(anyString())).thenReturn(sampleSort);
        when(motorcycleService.getAll(any(PageRequest.class))).thenReturn(motorcyclePage);

        mockMvc.perform(get("/motorcycles/admin"))
                .andExpect(status().isOk())
                .andExpect(view().name("motorcycle/admin/all"));
    }

    @Test
    void showAllInAdminPanel_shouldAddMotorcyclePageModelAttribute() throws Exception {

        Sort sampleSort = Sort.by(Sort.Direction.ASC, "model");

        Page<Motorcycle> motorcyclePage = new PageImpl<>(Arrays.asList(
                new Motorcycle(1L, "model1", new Brand()),
                new Motorcycle(2L, "model2", new Brand()),
                new Motorcycle(3L, "model3", new Brand())
        ));

        when(sortUtility.parseSort(anyString())).thenReturn(sampleSort);
        when(motorcycleService.getAll(any(PageRequest.class))).thenReturn(motorcyclePage);

        mockMvc.perform(get("/motorcycles/admin"))
                .andExpect(status().isOk())
                .andExpect(model().attribute("motorcyclePage", motorcyclePage));
    }

    @Test
    void showAllInAdminPanel_shouldMakePageRequestByProperPageNumberAndSort_whenRequestContainAppropriateParams() throws Exception {
        int pageNumber = 2;
        int pageSize = 20;
        String sortStr = "model:asc";
        Sort sort = Sort.by(Sort.Direction.ASC, "model");

        Page<Motorcycle> motorcyclePage = Page.empty();

        when(sortUtility.parseSort(sortStr)).thenReturn(sort);
        when(motorcycleService.getAll(any(PageRequest.class))).thenReturn(motorcyclePage);


        mockMvc.perform(get("/motorcycles/admin")
                        .param("pageNumber", String.valueOf(pageNumber))
                        .param("pageSize", String.valueOf(pageSize))
                        .param("sort", sortStr))
                .andExpect(status().isOk());

        verify(sortUtility).parseSort(sortStr);
        verify(motorcycleService).getAll(PageRequest.of(pageNumber, pageSize, sort));
    }

    @Test
    void showAllInAdminPanel_shouldAddPageNumberAndSortModelAttributes_whenRequestContainAppropriateParams() throws Exception {
        int pageNumber = 2;
        int pageSize = 20;
        String sortStr = "name:asc";
        Sort sampleSort = Sort.by(Sort.Direction.ASC, "model");

        Page<Motorcycle> motorcyclePage = Page.empty();

        when(sortUtility.parseSort(anyString())).thenReturn(sampleSort);
        when(motorcycleService.getAll(any(PageRequest.class))).thenReturn(motorcyclePage);


        mockMvc.perform(get("/motorcycles/admin")
                        .param("pageNumber", String.valueOf(pageNumber))
                        .param("pageSize", String.valueOf(pageSize))
                        .param("sort", sortStr))
                .andExpect(status().isOk())
                .andExpect(model().attribute("currentPageNumber", pageNumber))
                .andExpect(model().attribute("currentSort", sortStr));
    }

    @Test
    void showAllInAdminPanel_shouldAddDefaultPageNumberAndSortModelAttributes_whenRequestDoesNotContainAppropriateParams() throws Exception {

        Sort sampleSort = Sort.by(Sort.Direction.ASC, "model");
        Page<Motorcycle> motorcyclePage = Page.empty();

        when(sortUtility.parseSort(anyString())).thenReturn(sampleSort);
        when(motorcycleService.getAll(any(PageRequest.class))).thenReturn(motorcyclePage);

        mockMvc.perform(get("/motorcycles/admin"))
                .andExpect(status().isOk())
                .andExpect(model().attributeExists("currentPageNumber"))
                .andExpect(model().attributeExists("currentSort"));
    }

}