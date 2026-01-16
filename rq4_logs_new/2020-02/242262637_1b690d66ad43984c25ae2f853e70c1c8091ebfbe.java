package com.seblong.okr.controllers;

import com.seblong.okr.entities.Aligning;
import com.seblong.okr.entities.Employee;
import com.seblong.okr.entities.OKR;
import com.seblong.okr.services.AligningService;
import com.seblong.okr.services.EmployeeService;
import com.seblong.okr.services.OKRService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(value = "/aligning", produces = {MediaType.APPLICATION_JSON_VALUE})
public class APIAligningController {

    @Autowired
    private AligningService aligningService;

    @Autowired
    private EmployeeService employeeService;

    @Autowired
    private OKRService okrService;

    @PostMapping("/add")
    public Map<String, Object> add(
            @RequestParam(value = "employee") String employeeId,
            @RequestParam(value = "objective") String objectiveId,
            @RequestParam(value = "period") String periodId,
            @RequestParam(value = "targetE") String targetEId,
            @RequestParam(value = "targetO") String targetOId
    ){
        Map<String, Object> rMap = new HashMap<>(4);
        if(employeeService.findById(employeeId) == null){
            rMap.put("status", 404);
            rMap.put("message", "employee-not-exists");
            return rMap;
        }
        if(employeeService.findById(targetEId) == null){
            rMap.put("status", 404);
            rMap.put("message", "targetE-not-exists");
            return rMap;
        }
        Aligning aligning = aligningService.align(employeeId, objectiveId, periodId, targetEId, targetOId);
        rMap.put("status", 200);
        rMap.put("message", "OK");
        rMap.put("unique", aligning.getId().toString());
        return rMap;
    }

    @PostMapping("/remove")
    public Map<String, Object> remove(
            @RequestParam(value = "unique") String unique
    ){
        Map<String, Object> rMap = new HashMap<>(4);
        aligningService.remove(unique);
        rMap.put("status", 200);
        rMap.put("message", "OK");
        return rMap;
    }

    /**
     * 获取用户对齐
     * 包含上一级对齐和下一级
     * @param objectiveId
     * @return
     */
    @GetMapping("/get")
    public Map<String, Object> get(
            @RequestParam(value = "objective") String objectiveId
    ){
        Map<String, Object> rMap = new HashMap<>(4);
        Aligning aligningTop = aligningService.getTop(objectiveId);
        OKR.Objective objectiveTop = okrService.getObjective(aligningTop.getTargetE(), aligningTop.getPeriod(), aligningTop.getTargetO());
        Map<String, Object> top = new HashMap<>();
        if(objectiveTop != null){
            top.put("objective", objectiveTop);
            Employee employee = employeeService.findById(aligningTop.getTargetE());
            top.put("employee", employee);
        }
        rMap.put("aligningTop", top);
        List<Aligning> aligningList = aligningService.getChildren(objectiveId);
        List<Map<String, Object>> children = new ArrayList<>();
        if(aligningList != null){
            aligningList.forEach(aligning -> {
                Map<String, Object> map = new HashMap<>();
                Employee employee = employeeService.findById(aligning.getEmployee());
                map.put("employee", employee);
                OKR.Objective objective = okrService.getObjective(aligning.getEmployee(), aligning.getPeriod(), aligning.getObjective());
                map.put("objective", objective);
            });
        }
        rMap.put("aligningChildren", children);
        rMap.put("status", 200);
        rMap.put("message", "OK");
        return rMap;
    }

    /**
     * 获取用户对齐视图
     * @param objectiveId
     * @return
     */
    @GetMapping("/view")
    public Map<String, Object> view(
            @RequestParam(value = "objective") String objectiveId
    ){
        Map<String, Object> rMap = new HashMap<>(4);
        List<Aligning> aligningList = aligningService.getChildren(objectiveId);
        List<Map<String, Object>> children = new ArrayList<>();
        if(aligningList != null){
            aligningList.forEach(aligning -> {
                Map<String, Object> map = new HashMap<>();
                Employee employee = employeeService.findById(aligning.getEmployee());
                map.put("employee", employee);
                OKR.Objective objective = okrService.getObjective(aligning.getEmployee(), aligning.getPeriod(), aligning.getObjective());
                map.put("objective", objective);
            });
        }
        rMap.put("aligningChildren", children);
        rMap.put("status", 200);
        rMap.put("message", "OK");
        return rMap;
    }
}