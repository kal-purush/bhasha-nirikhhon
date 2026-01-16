package com.xianyo.util.commentmodule.controller;

import com.xianyo.loginmodule.dao.pojo.UserData;
import com.xianyo.util.commentmodule.dao.pojo.CommentData;
import com.xianyo.util.commentmodule.service.CommentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.sql.Timestamp;
import java.util.Date;

@Controller
public class CommentController {

    @Autowired
    CommentService commentService;

    /**
     * 跳转到写回答评论的视图
     *
     * @return 写评论的视图
     */
    @RequestMapping(value = "toAnswer.comment", method = RequestMethod.GET)
    public ModelAndView ToWriteCommentJSP() {
        ModelAndView mav = new ModelAndView("WriteComment");
        return mav;
    }

    /**
     *
     */
    @RequestMapping(value = "writeAnswer.comment", method = RequestMethod.POST)
    public ModelAndView WriteCommend(HttpServletRequest request, HttpSession session) {
        //将评论信息存储在服务器中
        CommentData newComment=CommentEncapsulation(request.getParameter("comment"),
                ((UserData)session.getAttribute("User")).getUserid());
        commentService.WriteComment(newComment);
        //界面显示
        ModelAndView mav = new ModelAndView("WriteComment");
        mav.addObject("newCommentMessage", request.getParameter("comment"));
        mav.addObject("person",
                ((UserData)session.getAttribute("User")).getUsername());
        return mav;
    }

    private CommentData CommentEncapsulation(String comment,long userid){
        CommentData tmpData=new CommentData();
        tmpData.setComment(comment);        //设置评论内容
        tmpData.setUserid(userid);          //设置评论用户ID
        tmpData.setCommenttime(new Date()); //设置发信时间
        return tmpData;
    }
}