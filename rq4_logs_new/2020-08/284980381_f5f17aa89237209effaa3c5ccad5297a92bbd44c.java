package com.autohome.api.controller;

import com.autohome.api.service.BrandLeveService;
import com.autohome.common.vo.R;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


/**
 * @author YMQ
 */

@RestController
@RequestMapping("/api/brandlevecontroller/")
public class BrandLeveController {

    @Autowired
    private BrandLeveService service;

    @GetMapping("getallcargeneral.do")
    public R getAllCarGeneral() {
        return service.getAllBrandLevel();
    }

    @PostMapping("getbrandbyId.do/{bid}")
    public R GetBrandById(@PathVariable Integer bid) {
        return service.GetBrandById(bid);
    }

    @GetMapping("getbrandidandmodellevelAndtype.do/{type}/{model_level}/{bid}")
    public R getBrandIdAndModelLevelAndType(Integer type, String model_level, Integer bid) {
        return service.getBrandIdAndModelLevelAndType(type, model_level, bid);
    }

    @GetMapping("getcarbybrandid.do/{bid}")
    public R getCarByBrandId(@PathVariable Integer bid) {
        return service.getCarByBrandId(bid);
    }
}