package com.allstate.compozed.springplayground.math;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.Map;

@RestController
final class MathController {

    @RequestMapping(path = "/math/square/{number}")
    Map<String, Integer> square(@PathVariable int number) {
        return Collections.singletonMap("square", number * number);
    }
}