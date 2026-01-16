package com.xmsmartcity.controller.project;

import com.alibaba.fastjson.JSON;
import com.xmsmartcity.mapper.TsRoleMapper;
import com.xmsmartcity.pojo.*;
import com.xmsmartcity.service.RoleService;
import com.xmsmartcity.util.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * Created by lingyun on 2018/2/6.
 */
@Controller
public class RoleController {


    @Autowired
    RoleService roleService;


    /*
    *  查找全部角色成员
     */
    @ResponseBody
    public CommonObjReturn getAllRoles(@RequestBody CommonObjParam objparam, HttpServletRequest request, HttpServletResponse response) {

        List<TsRole> list = roleService.findAllroles();
        CommonObjReturn commonObjReturn = new CommonObjReturn();
        commonObjReturn.setData(list);
        commonObjReturn.setResult("true");
        commonObjReturn.setResultTime(DateUtils.DateToString(new Date(), DateUtils.formatStr_yyyyMMddHHmmss));
        return commonObjReturn;
    }

    /*
   *  查找角色成员
    */
    @ResponseBody
    public CommonObjReturn getRoleByRoleName(@RequestBody CommonObjParam objparam, HttpServletRequest request, HttpServletResponse response) {

        HashMap obj = (HashMap)objparam.getData();
        String rolename = obj.get("rolename").toString();
        CommonObjReturn commonObjReturn = new CommonObjReturn();
        if(!rolename.isEmpty()){
            List<TsRole> list = roleService.findRolesByRoleName(rolename);
            commonObjReturn.setData(list);
            commonObjReturn.setResult("true");
        }else {
            commonObjReturn.setData(null);
            commonObjReturn.setResult("false");
        }

        commonObjReturn.setResultTime(DateUtils.DateToString(new Date(), DateUtils.formatStr_yyyyMMddHHmmss));
        return commonObjReturn;

    }

    /*
    *   删除角色
     */
    @ResponseBody
    public CommonObjReturn deleteRole(@RequestBody CommonObjParam objparam, HttpServletRequest request, HttpServletResponse response) {

        HashMap obj = (HashMap)objparam.getData();
        int id = Integer.parseInt(obj.get("id").toString());
        int index = roleService.deleteByPrimaryKey(id);
        CommonObjReturn commonObjReturn = new CommonObjReturn();
        if(index > 0){
            List<TsRole> list = roleService.findAllroles();
            commonObjReturn.setData(list);
            commonObjReturn.setResult("true");
        }else {
            commonObjReturn.setData(null);
            commonObjReturn.setResult("flase");
        }

        commonObjReturn.setResultTime(DateUtils.DateToString(new Date(), DateUtils.formatStr_yyyyMMddHHmmss));
        return commonObjReturn;

    }


    /*
    *   添加角色
     */
    @ResponseBody
    public CommonObjReturn addRole(@RequestBody CommonObjParam objparam, HttpServletRequest request, HttpServletResponse response) {

        TsRole tsRole = JSON.parseObject(JSON.toJSONString(objparam.getData()), TsRole.class);
//        TsRole sqlRole = roleService.selectByroleCode(tsRole.getRolecode());
        CommonObjReturn commonObjReturn = new CommonObjReturn();
//        if(sqlRole != null){
//            commonObjReturn.setData("角色编码已经被注册");
//            commonObjReturn.setResult("true");
//            return commonObjReturn;
//        }
        int index = roleService.insert(tsRole);
        if(index > 0){
            List<TsRole> list = roleService.findAllroles();
            commonObjReturn.setData(list);
            commonObjReturn.setResult("true");
        }else {
            commonObjReturn.setData(null);
            commonObjReturn.setResult("flase");
        }
        commonObjReturn.setResultTime(DateUtils.DateToString(new Date(), DateUtils.formatStr_yyyyMMddHHmmss));
        return commonObjReturn;

    }

    /*
   *   更新角色
    */
    @ResponseBody
    public CommonObjReturn updateRole(@RequestBody CommonObjParam objparam, HttpServletRequest request, HttpServletResponse response) {

        TsRole tsRole = JSON.parseObject(JSON.toJSONString(objparam.getData()), TsRole.class);
        int index = roleService.updateByPrimaryKey(tsRole);
        CommonObjReturn commonObjReturn = new CommonObjReturn();
        commonObjReturn.setData(null);
        commonObjReturn.setResult("true");
        commonObjReturn.setResultTime(DateUtils.DateToString(new Date(), DateUtils.formatStr_yyyyMMddHHmmss));
        return commonObjReturn;

    }


    //查询拥有某角色的用户信息
    public CommonObjReturn selectRoleByUserId(@RequestBody CommonObjParam objparam, HttpServletRequest request, HttpServletResponse response){

        HashMap obj = (HashMap)objparam.getData();
        int id = Integer.parseInt(obj.get("id").toString());
        TsUserRole userRole = roleService.selectRoleByUserId(id);
        CommonObjReturn commonObjReturn = new CommonObjReturn();
        commonObjReturn.setData(userRole);
        commonObjReturn.setResult("true");
        commonObjReturn.setResultTime(DateUtils.DateToString(new Date(), DateUtils.formatStr_yyyyMMddHHmmss));
        return commonObjReturn;
    }

    //查询某用户对应的角色

    public CommonObjReturn selectUserInfoByRoleId(@RequestBody CommonObjParam objparam, HttpServletRequest request, HttpServletResponse response){

        HashMap obj = (HashMap)objparam.getData();
        int id = Integer.parseInt(obj.get("id").toString());
        TsUserRole userRole = roleService.selectUserInfoByRoleId(id);
        CommonObjReturn commonObjReturn = new CommonObjReturn();
        commonObjReturn.setData(userRole);
        commonObjReturn.setResult("true");

        commonObjReturn.setResultTime(DateUtils.DateToString(new Date(), DateUtils.formatStr_yyyyMMddHHmmss));
        return commonObjReturn;
    }

}