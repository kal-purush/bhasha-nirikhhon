package com.example.demo.controller;

import com.example.demo.util.MathUtil;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/fibonacci")
public class FibonacciController {

  @GetMapping("/{index}")
  public Integer fibonacci(@PathVariable Integer index) {
    return MathUtil.fibonacci(index);
  }
}