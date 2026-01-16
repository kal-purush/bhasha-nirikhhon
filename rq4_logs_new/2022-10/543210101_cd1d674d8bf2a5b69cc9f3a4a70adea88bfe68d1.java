package com.job_search.controller;

import com.google.gson.Gson;
import com.job_search.controller.dto.CompanyDto;
import com.job_search.exception.DuplicateRecordException;
import com.job_search.mapper.CompanyMapper;
import com.job_search.repository.entity.Company;
import com.job_search.service.CompanyService;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import javax.persistence.EntityNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.job_search.DataProvider.getCompany;
import static java.lang.String.format;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ExtendWith(MockitoExtension.class)
class CompanyControllerUnitTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private CompanyService companyService;

    @Autowired
    private CompanyMapper companyMapper;

    @Test
    void testGetAllCompanies() throws Exception {
        List<Company> companyList = Arrays.asList(getCompany(), getCompany(), getCompany(), getCompany());
        List<CompanyDto> companyDtoList = companyList.stream().map(company -> companyMapper.toCompanyDto(company)).collect(Collectors.toList());
        when(companyService.getAllCompanies()).thenReturn(companyDtoList);
        mockMvc.perform(get("/api/companies"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$", hasSize(companyDtoList.size())));
    }

    @Test
    void testGetCompanyByName() throws Exception {
        CompanyDto companyDto = companyMapper.toCompanyDto(getCompany());
        when(companyService.getCompanyByName(companyDto.getName())).thenReturn(companyDto);
        mockMvc.perform(get("/api/companies/company").param("name", companyDto.getName()))
                .andExpect(status().isOk())
                .andExpect(content().string(new Gson().toJson(companyDto)));
    }

    @Test
    void testCreateCompany() throws Exception {
        CompanyDto companyDto = companyMapper.toCompanyDto(getCompany());
        when(companyService.existsByName(any(String.class))).thenReturn(false);
        when(companyService.createCompany(any(CompanyDto.class))).thenReturn(companyDto);
        mockMvc.perform(post("/api/companies").contentType(MediaType.APPLICATION_JSON).content(new Gson().toJson(companyDto)))
                .andExpect(status().isOk())
                .andExpect(content().string(new Gson().toJson(companyDto)));
    }

    @Test
    void negativeTestCreateCompany() throws Exception {
        CompanyDto companyDto = companyMapper.toCompanyDto(getCompany());
        when(companyService.existsByName(any(String.class))).thenReturn(true);
        when(companyService.createCompany(companyDto)).thenReturn(companyDto);
        MvcResult result = mockMvc.perform(post("/api/companies").contentType(MediaType.APPLICATION_JSON)
                .content(new Gson().toJson(companyDto))).andReturn();
        assertThat(result.getResponse().getStatus()).isEqualTo(HttpStatus.BAD_REQUEST.value());
        assertThat(result.getResolvedException()).isInstanceOf(DuplicateRecordException.class);
        assertThat(Objects.requireNonNull(result.getResolvedException()).getMessage()).isEqualTo(format("The '%s' company already exist", companyDto.getName()));
    }

    @Test
    void testDeleteCompany() throws Exception {
        when(companyService.existsByName(any(String.class))).thenReturn(true);
        mockMvc.perform(delete(format("/api/companies/company/%s", getCompany().getName())))
                .andExpect(status().isNoContent());
    }

    @Test
    void negativeTestDeleteCompany() throws Exception {
        CompanyDto companyDto = companyMapper.toCompanyDto(getCompany());
        when(companyService.existsByName(any(String.class))).thenReturn(false);
        MvcResult result = mockMvc.perform(delete(format("/api/companies/company/%s", companyDto.getName()))).andReturn();
        assertThat(result.getResponse().getStatus()).isEqualTo(HttpStatus.NOT_FOUND.value());
        assertThat(result.getResolvedException()).isInstanceOf(EntityNotFoundException.class);
        assertThat(Objects.requireNonNull(result.getResolvedException()).getMessage()).isEqualTo(format("The '%s' company not found", companyDto.getName()));
    }

    @Test
    void testUpdateCompany() throws Exception {
        CompanyDto companyDto = companyMapper.toCompanyDto(getCompany());
        when(companyService.existsByName(any(String.class))).thenReturn(true);
        when(companyService.updateCompanyByName(any(String.class), any(CompanyDto.class))).thenReturn(companyDto);
        mockMvc.perform(put("/api/companies/company/Apple")
                .contentType(MediaType.APPLICATION_JSON).content(new Gson().toJson(companyDto)))
                .andExpect(status().isOk())
                .andExpect(content().string(new Gson().toJson(companyDto)));
    }

    @Test
    void negativeTestUpdateCompany() throws Exception {
        CompanyDto companyDto = companyMapper.toCompanyDto(getCompany());
        when(companyService.existsByName(any(String.class))).thenReturn(false);
        MvcResult result = mockMvc.perform(put("/api/companies/company/Apple")
                .contentType(MediaType.APPLICATION_JSON).content(new Gson().toJson(companyDto))).andReturn();
        assertThat(result.getResponse().getStatus()).isEqualTo(HttpStatus.NOT_FOUND.value());
        assertThat(result.getResolvedException()).isInstanceOf(EntityNotFoundException.class);
        assertThat(Objects.requireNonNull(result.getResolvedException()).getMessage()).isEqualTo("The 'Apple' company not found");
    }
}