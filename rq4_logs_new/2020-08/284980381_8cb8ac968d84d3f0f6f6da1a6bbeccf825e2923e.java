package com.autohome.api.controller;

import com.autohome.api.service.SearchService;
import com.autohome.common.vo.R;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Class: SearchController
 * @Description:
 * @Author: Jerry(姜源)
 * @Create: 2020/08/10 21:18
 */
@Api(tags = "搜索模块接口-ES")
@RestController
@RequestMapping("/api/search")
public class SearchController {

    @Autowired
    private SearchService service;

    @ApiOperation(value = "获取所有新闻信息", notes = "默认按照分页获取所有新闻信息")
    @GetMapping("/getall.do")
    public R getAll(String msg, int page, int limit) {
        return service.queryPage(msg, page, limit);
    }

}