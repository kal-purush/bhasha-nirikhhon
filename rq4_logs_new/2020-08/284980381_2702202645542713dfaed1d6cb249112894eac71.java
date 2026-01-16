package com.autohome.api.controller;

import com.autohome.api.service.CarLevelService;
import com.autohome.common.vo.R;
import com.autohome.entity.MainLevel;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * @Class: CarLevelController
 * @Description: 首页汽车主分类信息CRUD
 * @Author: Jerry(姜源)
 * @Create: 2020/08/07 14:59
 */
@Api(tags = "首页汽车主分类接口")
@RestController
@RequestMapping("/api/carlevel")
public class CarLevelController {

    @Autowired
    private CarLevelService service;

    /**
     * 新增一个分类
     *
     * @param name 分类名称
     * @return
     */
    @ApiOperation(value = "新增1个分类", notes = "新增1个分类，需要传入一个分类名称")
    @GetMapping("/add/{name}")
    public R addLevel(@PathVariable String name) {
        return service.addLevel(name);
    }

    /**
     * 删除一个分类
     *
     * @param id 分类编号
     * @return
     */
    @ApiOperation(value = "删除1个分类", notes = "删除1个分类，需要传入分类的编号")
    @GetMapping("/del/{id}")
    public R delLevel(@PathVariable Integer id) {
        return service.delLevel(id);
    }

    /**
     * 更改一个分类信息
     *
     * @return
     */
    @ApiOperation(value = "修改1个分类", notes = "修改1个分类，需要传入分类编号和分类名称")
    @PostMapping(value = "/upd")
    public R updateLevel(@RequestBody MainLevel mainLevel) {
        return service.updateLevel(mainLevel);
    }

    /**
     * 获取所有分类信息
     *
     * @return
     */
    @ApiOperation(value = "获取全部主分类", notes = "获取全部主分类信息")
    @GetMapping("/all")
    public R getAllLevels() {
        return service.getAllLevels();
    }
}