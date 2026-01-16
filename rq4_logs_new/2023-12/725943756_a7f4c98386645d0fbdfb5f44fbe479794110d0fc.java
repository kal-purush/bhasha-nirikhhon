package com.keyi.db_goods.controller;

import com.keyi.db_goods.entity.Good;
import com.keyi.db_goods.mapper.GoodMapper;
import com.keyi.db_goods.service.GoodService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/good")
public class GoodController {
    @Autowired
    private GoodMapper goodMapper;
    @Autowired
    private GoodService goodService;

    // 1、增加
    @PostMapping
    public String add(@RequestBody Good good) {
        return goodService.add(good);
    }

    // 2、删除
    @DeleteMapping("/id/{id}")
    public String deleteById(@PathVariable Integer id) {
        return goodService.deleteById(id);
    }

    @DeleteMapping("/name/{name}")
    public String deleteByName(@PathVariable String name) {
        return goodService.deleteByName(name);
    }

    // 3、修改
    @PostMapping("/update")
    public String update(@RequestBody Good good) {
        return goodService.update(good);
    }

    // 4、查询
    @GetMapping("/")
    public List<Good> getClientAll() {
        return goodMapper.getGoodAll();
    }
}