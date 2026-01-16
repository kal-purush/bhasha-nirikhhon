package com.autohome.api.controller;

import com.autohome.api.service.UsedCarConfigService;
import com.autohome.common.dto.LicencePlateDto;
import com.autohome.common.jsonpojo.LicencePlate;
import com.autohome.common.vo.R;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * @program: car_home
 * @description:
 * @author: ztk
 * @create: 2020-08-12 15:18
 **/

@RestController
@Api(tags = "二手车详情")
@RequestMapping("/api/usedcar/config")
public class UsedCarConfigController {
    @Autowired
    private UsedCarConfigService service;

    /**
     *
     * @param cid
     * @param uid
     * @return
     */
    @PostMapping("/add.do/{cid}/{uid}")
    public R addUsedCarConfig(@PathVariable Integer cid, @PathVariable Integer uid, @RequestBody LicencePlate licencePlate) {
        return service.insertUsedCarConfig(cid, uid, licencePlate);
    }

    /**
     *
     * @param cid
     * @param uid
     * @param uniqueId
     * @return
     */
    @PostMapping("/del.do/{cid}/{uid}/{uniqueId}")
    public R deleteCarConfig(@PathVariable Integer cid, @PathVariable Integer uid, @PathVariable int uniqueId) {
        return service.deleteUsedCarConfig(uid, cid, uniqueId);
    }

    /**
     * 查询该用户名下所有二手车汽车详情/查询该用户名下该型号所有的二手车汽车详情
     * @param cid 汽车型号id (可为空)
     * @param uid 用户id
     * @return R
     */
    @GetMapping(value = {"/getdetail.do/{uid}/{cid}", "/getdetail.do/{uid}"})
    public R getCarConfig(@PathVariable Integer uid, @PathVariable(value = "cid",required = false) Integer cid) {
        return service.getUsedCarConfig(cid, uid);
    }

    /**
     *
     * @param licencePlateDto
     * @return
     */
    @PostMapping("/update.do")
    public R updateLicencePlateInfo(@RequestBody LicencePlateDto licencePlateDto) {
        return service.updateLicencePlate(licencePlateDto);
    }
}