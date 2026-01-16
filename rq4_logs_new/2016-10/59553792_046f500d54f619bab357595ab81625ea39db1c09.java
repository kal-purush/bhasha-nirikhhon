/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.main.controller;

import com.main.model.BrandDetails;
import com.main.model.CarPartInfo;
import com.main.model.CarPartVault;
import com.main.service.AllInsertService;
import com.main.service.AllViewService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

/**
 *
 * @author krisenla
 */
@Controller
public class OldSparePartsController {

    @Autowired
    AllViewService viewService;

    @Autowired
    AllInsertService insertService;

    @Autowired
    Environment env;

    //view sparepart old part vehicle code goes here
    @RequestMapping(value = "viewOldPartVehicleList")
    public ModelAndView viewOldPartVehicleList() {
        ModelAndView modelAndView = new ModelAndView("ViewOldPartVehicleList");
        modelAndView.addObject("sparesVehicleDetails", viewService.getanyjdbcdatalist("SELECT *,bd.id as branddetailid FROM branddetails bd\n"
                + "inner join brand br on br.id=bd.brandid\n"
                + "where bd.isdelete='No' and bd.id not in ('" + env.getProperty("generic_brand_detailid") + "','" + env.getProperty("consumable_brand_detailid") + "')"));
        return modelAndView;
    }
    //view sparepart old part vehicle code ends! here

    //redirects to add new car part page with category details in it
    @RequestMapping("createOldCarParts")
    public ModelAndView createOldCarParts() {
        ModelAndView modelAndView = new ModelAndView("AddOldCarParts");
        modelAndView.addObject("catdtls", viewService.getanyhqldatalist("from category where isdelete<>'Yes'"));
        return modelAndView;
    }
    
    @RequestMapping(value = "insertoldcarparts", method = RequestMethod.POST)
    public String insertoldcarparts(@ModelAttribute CarPartVault carPartVault) {
        String pre = env.getProperty("carpartvault");
        String id = pre + insertService.getmaxcount("carpartvault", "id", 5);
        carPartVault.setId(id);

        insertService.insert(carPartVault);

        //adds to brand for the new Car Part inserted
        List<BrandDetails> brandDetailsList = viewService.getanyhqldatalist("from branddetails where isdelete='No' and id not in ('" + env.getProperty("generic_brand_detailid") + "','" + env.getProperty("consumable_brand_detailid") + "')");
        for (int i = 0; i < brandDetailsList.size(); i++) {
            if (!brandDetailsList.get(i).getId().equals(env.getProperty("generic_brand_detailid"))) {
                CarPartInfo carPartInfo = new CarPartInfo();
                carPartInfo.setBranddetailid(brandDetailsList.get(i).getId());
                carPartInfo.setVaultid(id);
                String pre2 = env.getProperty("carpartinfo");
                String carPartInfoid = insertService.getmaxcount("carpartinfo", "id", 5);
                String maxCountCarPartInfoid = pre2 + carPartInfoid;
                carPartInfo.setId(maxCountCarPartInfoid);
                carPartInfo.setBalancequantity("0");
                insertService.insert(carPartInfo);
            }
        }

        return "redirect:viewOldPartVehicleList";
    }

    //view viewSparesList for the vehicle model selected
    @RequestMapping(value = "viewOldSparesList")
    public ModelAndView viewOldSparesList(@RequestParam(value = "id") String branddetailid) {
        ModelAndView modelAndView = new ModelAndView("ViewOldSparepartList");
        modelAndView.addObject("taxdt", viewService.getanyjdbcdatalist("SELECT * from taxes where isdelete<>'Yes' and id not in('LTX2','LTX3','LTX4')"));
        modelAndView.addObject("sparepartdetails", viewService.getanyjdbcdatalist("SELECT cpi.*,cpv.name as partname,cg.name as categoryname FROM carpartinfo cpi \n"
                + "inner join carpartvault cpv on cpv.id=cpi.vaultid\n"
                + "inner join category cg on cg.id=cpv.categoryid\n"
                + "where cpi.branddetailid='" + branddetailid + "' and cpi.isdelete='No' and cpi.balancequantity>='0' and cpv.isold='Yes'"));
        modelAndView.addObject("negativesparepartdetails", viewService.getanyjdbcdatalist("SELECT cpi.*,cpv.name as partname,cg.name as categoryname FROM carpartinfo cpi \n"
                + "inner join carpartvault cpv on cpv.id=cpi.vaultid\n"
                + "inner join category cg on cg.id=cpv.categoryid\n"
                + "where cpi.branddetailid='" + branddetailid + "' and cpi.isdelete='No' and cpi.balancequantity<'0' and cpv.isold='Yes'"));
        return modelAndView;
    }
    
}