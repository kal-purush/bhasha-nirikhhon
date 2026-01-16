package com.university.contractors.controller;

import com.university.contractors.config.Endpoints;
import com.university.contractors.controller.payload.SearchStudent;
import com.university.contractors.repository.SearchStudentRepository;
import com.university.contractors.repository.SearchStudentResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class SearchStudentController {

    private final SearchStudentRepository searchStudentRepository;

    @Autowired
    public SearchStudentController(SearchStudentRepository searchStudentRepository) {
        this.searchStudentRepository = searchStudentRepository;
    }

    @PostMapping(path = Endpoints.SEARCH)
    public List<SearchStudentResult> search(@RequestBody SearchStudent searchStudent) {
        return searchStudentRepository.search(searchStudent);
    }
}