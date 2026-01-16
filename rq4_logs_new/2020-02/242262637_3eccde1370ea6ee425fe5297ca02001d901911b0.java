package com.seblong.okr.controllers;

import com.seblong.okr.entities.Comment;
import com.seblong.okr.entities.Employee;
import com.seblong.okr.resource.StandardEntityResource;
import com.seblong.okr.resource.StandardListResource;
import com.seblong.okr.resource.StandardRestResource;
import com.seblong.okr.services.CommentService;
import com.seblong.okr.services.EmployeeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping(value = "/comment", produces = {MediaType.APPLICATION_JSON_VALUE})
public class APICommentController {

    @Autowired
    private CommentService commentService;

    @Autowired
    private EmployeeService employeeService;

    /**
     * 添加评论
     * @param employeeId
     * @param period
     * @param ownerId
     * @param content
     * @return
     */
    @PostMapping("/add")
    public ResponseEntity<StandardRestResource> comment(
            @RequestParam(value = "employee") String employeeId,
            @RequestParam(value = "period") String period,
            @RequestParam(value = "owner") String ownerId,
            @RequestParam(value = "content") String content){
        Employee employee =  employeeService.findById(employeeId);
        if(employee == null){
            return new ResponseEntity<StandardRestResource>(new StandardRestResource(404, "employee-not-exists"), HttpStatus.OK);
        }
        Employee owner = employeeService.findById(ownerId);
        if(owner == null){
            return new ResponseEntity<StandardRestResource>(new StandardRestResource(404, "owner-not-exists"), HttpStatus.OK);
        }
        Comment comment = commentService.comment(employeeId, period, ownerId, content);
        comment.setThumb_avatar(employee.getThumb_avatar());
        comment.setAvatar(employee.getAvatar());
        comment.setName(employee.getName());
        return new ResponseEntity<StandardRestResource>(new StandardEntityResource<Comment>(comment), HttpStatus.OK);
    }

    /**
     * 删除评论
     * @param unique
     * @return
     */
    @PostMapping("/remove")
    public ResponseEntity<StandardRestResource> remove(
            @RequestParam(value = "unique") String unique){
        commentService.remove(unique);
        return new ResponseEntity<StandardRestResource>(new StandardRestResource(200, "OK"), HttpStatus.OK);
    }

    /**
     * 获取评论列表
     * @param employeeId
     * @param period
     * @return
     */
    @GetMapping("/list")
    public ResponseEntity<StandardRestResource> list(
            @RequestParam(value = "employee") String employeeId,
            @RequestParam(value = "period") String period,
            @RequestParam(value = "status") String status){
        List<Comment> commentList = commentService.list(employeeId, period, status);
        if(commentList == null) {
            commentList = Collections.emptyList();
        }
        commentList.forEach(comment -> {
            Employee employee = employeeService.findById(comment.getEmployee());
            comment.setName(employee.getName());
            comment.setAvatar(employee.getAvatar());
            comment.setThumb_avatar(employee.getThumb_avatar());
        });
        return new ResponseEntity<StandardRestResource>(new StandardListResource<Comment>(commentList), HttpStatus.OK);
    }

    /**
     * 标记为解决并隐藏
     * @param unique
     * @return
     */
    @PostMapping("/solve")
    public ResponseEntity<StandardRestResource> solve(
            @RequestParam(value = "unique") String unique){
        commentService.solve(unique);
        return new ResponseEntity<StandardRestResource>(new StandardRestResource(200, "OK"), HttpStatus.OK);
    }

    /**
     * 重新打开
     * @param unique
     * @return
     */
    @PostMapping("/reopen")
    public ResponseEntity<StandardRestResource> reopen(
            @RequestParam(value = "unique") String unique){
        commentService.reopen(unique);
        return new ResponseEntity<StandardRestResource>(new StandardRestResource(200, "OK"), HttpStatus.OK);
    }
}