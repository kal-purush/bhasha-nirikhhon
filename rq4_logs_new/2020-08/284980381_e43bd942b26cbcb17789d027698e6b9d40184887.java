package com.autohome.api.controller;

import com.autohome.api.service.CarServiceService;
import com.autohome.common.dto.ServiceFirstLevelDto;
import com.autohome.common.vo.R;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * @Class: CarServiceController
 * @Description:
 * @Author: Jerry(姜源)
 * @Create: 2020/08/08 09:50
 */
@Api(tags = "汽车服务模块接口")
@RestController
@RequestMapping("/api/carservice")
public class CarServiceController {

    @Autowired
    private CarServiceService service;

    /**
     * 新增一个服务
     *
     * @param name 服务名称
     * @return
     */
    @ApiOperation(value = "新增1个服务", notes = "新增1个服务，需要传入一个服务名称")
    @GetMapping("/add/{name}")
    public R addService(@PathVariable String name) {
        return service.addService(name);
    }

    /**
     * 删除一个服务
     *
     * @param id 服务编号
     * @return
     */
    @ApiOperation(value = "删除1个服务", notes = "删除1个服务，需要传入服务的编号")
    @GetMapping("/del/{id}")
    public R delService(@PathVariable Integer id) {
        return service.delService(id);
    }

    /**
     * 更改一个服务信息
     *
     * @return
     */
    @ApiOperation(value = "修改1个服务", notes = "修改1个服务，需要传入服务编号和服务名称")
    @PostMapping("/upd")
    public R updateService(@RequestBody ServiceFirstLevelDto dto) {
        return service.updateService(dto);
    }

    /**
     * 获取所有服务信息
     *
     * @return
     */
    @ApiOperation(value = "获取全部主服务", notes = "获取全部主服务信息")
    @GetMapping("/all")
    public R getAllServices() {
        return service.getAllServices();
    }

    /**
     * 根据编号获取单个服务名称
     *
     * @return
     */
    @ApiOperation(value = "根据编号获取服务", notes = "如果有需要，根据编号获取单个服务信息")
    @GetMapping("/getServiceById")
    public R getServiceById(Integer id) {
        return service.getServiceById(id);
    }
}