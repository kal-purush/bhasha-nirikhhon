package com.project.teamup.controller;

import com.project.teamup.dto.CompanyDTO;
import com.project.teamup.mapper.CompanyMapper;
import com.project.teamup.service.CompanyService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/company")
public class CompanyController {
    @Autowired
    private CompanyService companyService;
    @Autowired
    private CompanyMapper companyMapper;

    @PostMapping(value = "/save")
    public CompanyDTO saveCompany(@RequestBody CompanyDTO companyDTO) {
        return companyMapper.toDto(companyService.save(companyMapper.toEntity(companyDTO)));
    }

    @GetMapping
    public List<CompanyDTO> getAll() {
        return companyMapper.toDtoList(companyService.getAll());
    }

    @GetMapping("/{id}")
    public CompanyDTO findById(@PathVariable("id") Long id) {
        return companyService.findById(id)
                .map(company -> companyMapper.toDto(company))
                .orElse(null);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(value = HttpStatus.OK)
    public void deleteCompany(@PathVariable("id") Long id) {
        companyService.delete(id);
    }
}