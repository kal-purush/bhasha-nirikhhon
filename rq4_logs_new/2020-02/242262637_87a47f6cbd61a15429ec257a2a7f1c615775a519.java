package com.seblong.okr.controllers;

import com.seblong.okr.entities.Employee;
import com.seblong.okr.entities.Follow;
import com.seblong.okr.resource.StandardEntityResource;
import com.seblong.okr.resource.StandardListResource;
import com.seblong.okr.resource.StandardRestResource;
import com.seblong.okr.services.EmployeeService;
import com.seblong.okr.utils.OAuth2Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(value = "/employee", produces = {MediaType.APPLICATION_JSON_VALUE})
public class APIEmployeeController {

    @Autowired
    private EmployeeService employeeService;

    /**
     * 获取网页授权链接
     *
     * @return
     */
    @GetMapping(value = "/oauth")
    public Map<String, Object> getOAuth2Url() {
        Map<String, Object> rMap = new HashMap<>(4);
        String oauthUrl = employeeService.getAuth2Url();
        rMap.put("status", 200);
        rMap.put("message", "OK");
        rMap.put("oauthUrl", oauthUrl);
        return rMap;
    }

    /**
     * 根据code或者cookie获取员工信息，相当于登录
     * cookie为员工id
     *
     * @param code
     * @param cookie
     * @return
     */
    @PostMapping(value = "/login")
    public ResponseEntity<StandardRestResource> getEmployee(
            @RequestParam(value = "code", required = false) String code,
            @RequestParam(value = "cookie", required = false) String cookie) {
        Employee employee = employeeService.getEmployee(code, cookie);
        if(employee == null){
            return new ResponseEntity<StandardRestResource>(new StandardRestResource(406, "oauth-error"), HttpStatus.OK);
        }
        return new ResponseEntity<StandardRestResource>(new StandardEntityResource<Employee>(employee), HttpStatus.OK);
    }

    /**
     * 根据用户名前缀模糊查询用户
     * @param keyword
     * @return
     */
    @GetMapping(value = "/query")
    public ResponseEntity<StandardRestResource> query(@RequestParam(value = "keyword") String keyword) {
        List<Employee> employeeList = employeeService.queryByName(keyword);
        if (employeeList == null) {
            employeeList = Collections.emptyList();
        }
        return new ResponseEntity<StandardRestResource>(new StandardListResource<Employee>(employeeList), HttpStatus.OK);
    }

    /**
     * 根据用户id获取用户信息
     * @param unique
     * @return
     */
    @GetMapping("/get")
    public ResponseEntity<StandardRestResource> find(
            @RequestParam(value = "unique") String unique){
        Employee employee = employeeService.findById(unique);
        if(employee == null){
            return new ResponseEntity<StandardRestResource>(new StandardRestResource(404, "employee-not-exists"), HttpStatus.OK);
        }else {
            return new ResponseEntity<StandardRestResource>(new StandardEntityResource<Employee>(employee), HttpStatus.OK);
        }
    }

    /**
     * 关注用户
     * @param fromId
     * @param targetId
     * @return
     */
    @PostMapping("/follow")
    public Map<String, Object> follow(
            @RequestParam(value = "from") String fromId,
            @RequestParam(value = "target") String targetId){
        Map<String, Object> rMap = new HashMap<>(4);
        Employee from = employeeService.findById(fromId);
        if(from == null){
            rMap.put("status", 404);
            rMap.put("message", "from-error");
            return rMap;
        }
        Employee target = employeeService.findById(targetId);
        if(target == null){
            rMap.put("status", 404);
            rMap.put("message", "target-error");
            return rMap;
        }
        employeeService.follow(from, target);
        rMap.put("status", 200);
        rMap.put("message", "OK");
        return rMap;
    }

    /**
     * 取消关注
     * @param unique
     * @return
     */
    @PostMapping("/unfollow")
    public Map<String, Object> unFollow(
            @RequestParam(value = "unique") String unique){
        Map<String, Object> rMap = new HashMap<>(4);
        employeeService.unFollow(unique);
        rMap.put("status", 200);
        rMap.put("message", "OK");
        return rMap;
    }

    /**
     * 获取用户关注
     * @param employeeId
     * @return
     */
    @GetMapping("/get/follow")
    public ResponseEntity<StandardRestResource> getFollows(
            @RequestParam(value = "employee") String employeeId
    ){
        List<Follow> follows = employeeService.getFollows(employeeId);
        if(follows == null){
            follows = Collections.emptyList();
        }
        return new ResponseEntity<StandardRestResource>(new StandardListResource<Follow>(follows), HttpStatus.OK);
    }

}