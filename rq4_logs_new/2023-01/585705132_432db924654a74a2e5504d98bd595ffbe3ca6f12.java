package com.umc.accountbook.controller;

import com.umc.accountbook.domain.ExampleDomain;
import com.umc.accountbook.service.ExampleService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class ExampleController {

    private final ExampleService testService;

    @GetMapping("/example")
    public String example1() {
        return "This is example";
    }

    @GetMapping("/example/{id}")
    public ExampleDomain example2(@PathVariable Long id) {
        ExampleDomain exDomain = testService.getExampleDomain(id);

        return exDomain;
    }
}