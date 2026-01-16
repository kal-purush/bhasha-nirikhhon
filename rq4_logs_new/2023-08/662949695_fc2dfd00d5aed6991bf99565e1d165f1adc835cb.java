package edu.zuel.hahasearch.controller;

import edu.zuel.hahasearch.common.BaseResponse;
import edu.zuel.hahasearch.common.ErrorCode;
import edu.zuel.hahasearch.common.ResultUtils;
import edu.zuel.hahasearch.exception.BusinessException;
import edu.zuel.hahasearch.model.domain.User;
import edu.zuel.hahasearch.model.request.*;
import edu.zuel.hahasearch.service.SpiderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import static edu.zuel.hahasearch.common.ErrorCode.PARAMS_ERROR;
import static edu.zuel.hahasearch.constant.UserConstant.USER_LOGIN_STATE;

@RestController
@RequestMapping("/spider")
@Slf4j
public class SpiderController {
    @Resource
    private SpiderService spiderService;

    @PostMapping("/http")
    public BaseResponse<String> httpSpider(@RequestBody SpiderHttpRequest spiderHttpRequest, HttpServletRequest request) {
        Object userObj = request.getSession().getAttribute(USER_LOGIN_STATE);
        User currentUser = (User) userObj;
        if (currentUser == null) {
            throw new BusinessException(ErrorCode.NOT_LOGIN);
        }
        if (spiderHttpRequest == null) {
            return ResultUtils.error(PARAMS_ERROR);
        }
        String target = spiderHttpRequest.getTarget();
        String name = spiderHttpRequest.getName();
        String index = spiderHttpRequest.getIndex();
        String code = currentUser.getTenantCode();
        String deep = spiderHttpRequest.getDeep();
        if (target == null || name == null || index == null || code == null) {
            return ResultUtils.error(PARAMS_ERROR);
        }
        try {
            String msg = spiderService.httpSpider(target, name, index, code, deep);
            return ResultUtils.success(msg);
        } catch (Exception e) {
            return ResultUtils.error(ErrorCode.SYSTEM_ERROR);
        }
    }

    @PostMapping("/pause_task")
    public BaseResponse<String> pauseTask(@RequestBody SpiderPauseRequest spiderPauseRequest, HttpServletRequest request) {
        Object userObj = request.getSession().getAttribute(USER_LOGIN_STATE);
        User currentUser = (User) userObj;
        if (currentUser == null) {
            throw new BusinessException(ErrorCode.NOT_LOGIN);
        }

        if (spiderPauseRequest == null) {
            return ResultUtils.error(PARAMS_ERROR);
        }
        String code = currentUser.getTenantCode();
        String index = spiderPauseRequest.getIndex();
        if (code == null || index == null) {
            return ResultUtils.error(PARAMS_ERROR);
        }
        try {
            String msg = spiderService.pauseTask(code, index);
            return ResultUtils.success(msg);
        } catch (Exception e) {
            return ResultUtils.error(ErrorCode.SYSTEM_ERROR);
        }
    }

    @PostMapping("/cancel_task")
    public BaseResponse<String> cancelTask(@RequestBody SpiderCancelRequest spiderCancelRequest, HttpServletRequest request) {
        Object userObj = request.getSession().getAttribute(USER_LOGIN_STATE);
        User currentUser = (User) userObj;
        if (currentUser == null) {
            throw new BusinessException(ErrorCode.NOT_LOGIN);
        }
        if (spiderCancelRequest == null) {
            return ResultUtils.error(PARAMS_ERROR);
        }
        String code = currentUser.getTenantCode();
        String index = spiderCancelRequest.getIndex();
        if (code == null || index == null) {
            return ResultUtils.error(PARAMS_ERROR);
        }
        try {
            String msg = spiderService.cancelTask(code, index);
            return ResultUtils.success(msg);
        } catch (Exception e) {
            return ResultUtils.error(ErrorCode.SYSTEM_ERROR);
        }
    }

    @PostMapping("/resume_task")
    public BaseResponse<String> resumeTask(@RequestBody SpiderResumeRequest spiderResumeRequest, HttpServletRequest request) {
        Object userObj = request.getSession().getAttribute(USER_LOGIN_STATE);
        User currentUser = (User) userObj;
        if (currentUser == null) {
            throw new BusinessException(ErrorCode.NOT_LOGIN);
        }
        if (spiderResumeRequest == null) {
            return ResultUtils.error(PARAMS_ERROR);
        }
        String code = currentUser.getTenantCode();
        String index = spiderResumeRequest.getIndex();
        if (code == null || index == null) {
            return ResultUtils.error(PARAMS_ERROR);
        }
        try {
            String msg = spiderService.resumeTask(code, index);
            return ResultUtils.success(msg);
        } catch (Exception e) {
            return ResultUtils.error(ErrorCode.SYSTEM_ERROR);
        }
    }

    @PostMapping("get_tasks")
    public BaseResponse<String> getTasks(HttpServletRequest request) {
        Object userObj = request.getSession().getAttribute(USER_LOGIN_STATE);
        User currentUser = (User) userObj;
        if (currentUser == null) {
            throw new BusinessException(ErrorCode.NOT_LOGIN);
        }
        String code = currentUser.getTenantCode();
        try {
            String msg = spiderService.getTasks(code);
            return ResultUtils.success(msg);
        } catch (Exception e) {
            return ResultUtils.error(ErrorCode.SYSTEM_ERROR);
        }
    }

    @PostMapping("get_running_tasks")
    public BaseResponse<String> getRunningTasks(HttpServletRequest request) {
        Object userObj = request.getSession().getAttribute(USER_LOGIN_STATE);
        User currentUser = (User) userObj;
        if (currentUser == null) {
            throw new BusinessException(ErrorCode.NOT_LOGIN);
        }
        String code = currentUser.getTenantCode();
        try {
            String msg = spiderService.getRunningTasks(code);
            return ResultUtils.success(msg);
        } catch (Exception e) {
            return ResultUtils.error(ErrorCode.SYSTEM_ERROR);
        }
    }

    @PostMapping("get_waiting_tasks")
    public BaseResponse<String> getWaitingTasks(HttpServletRequest request) {
        Object userObj = request.getSession().getAttribute(USER_LOGIN_STATE);
        User currentUser = (User) userObj;
        if (currentUser == null) {
            throw new BusinessException(ErrorCode.NOT_LOGIN);
        }
        String code = currentUser.getTenantCode();
        try {
            String msg = spiderService.getWaitingTasks(code);
            return ResultUtils.success(msg);
        } catch (Exception e) {
            return ResultUtils.error(ErrorCode.SYSTEM_ERROR);
        }
    }

}