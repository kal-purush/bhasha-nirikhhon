package com.matrix.matrix_160_demo.nicatController;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RequestMapping("/member")
@RestController
@RequiredArgsConstructor
public class MemberController {

    @GetMapping
    public Integer getInteger(@RequestParam Integer id) {
        return id;
    }
}