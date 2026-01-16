package com.controller;
import com.model.pojo.Office;
import com.service.IOfficeService;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;
import javax.annotation.Resource;
import java.util.List;

@Controller
@RequestMapping(value = "/office")
public class OfficeController {

    @Resource
    private IOfficeService officeService;

    /**
     * 查询所有的科室
     * @return
     */
    @RequestMapping("/findOffice")
    public ModelAndView findAll() {
        ModelAndView modelAndView = new ModelAndView();

        List<Office> officeList  = officeService.findAll();
        modelAndView.addObject("officeList", officeList);
        modelAndView.setViewName("office");
        return modelAndView;
    }

    /**
     * 删除单个科室
     * @return
     */
    @RequestMapping("/deleteOffice")
    public ModelAndView deleteOffice(Integer offId) {
        System.out.println("offId===================="+offId);
        ModelAndView modelAndView = new ModelAndView();
        boolean flag= officeService.deleteOffice(offId);
        System.out.println("flag===================="+flag);

        List<Office> officeList  = officeService.findAll();
        modelAndView.addObject("officeList", officeList);
        modelAndView.setViewName("office");
        return modelAndView;
    }


    /**
     * 进入新增科室页面
     * @return
     */
    @RequestMapping("/toAddOffice")
    public ModelAndView toAddOffice() {
        System.out.println("========进入到新方法");
        ModelAndView modelAndView = new ModelAndView();
        modelAndView.setViewName("addOffice");
        return modelAndView;
    }

    @RequestMapping("/addOffice")
    public ModelAndView addOffice(String offName) {
        System.out.println("新增科室名字===================="+offName);
        boolean flag = officeService.insertOffice(offName);
        System.out.println("flag===================="+flag);

        ModelAndView modelAndView = new ModelAndView();
        List<Office> officeList  = officeService.findAll();
        modelAndView.addObject("officeList", officeList);
        modelAndView.setViewName("office");
        return modelAndView;
    }












}