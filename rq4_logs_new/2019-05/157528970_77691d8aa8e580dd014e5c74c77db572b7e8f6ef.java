package com.smile.start.controller.project;

import com.github.pagehelper.PageInfo;
import com.smile.start.commons.FastJsonUtils;
import com.smile.start.commons.LoggerUtils;
import com.smile.start.controller.BaseController;
import com.smile.start.model.auth.User;
import com.smile.start.model.base.PageRequest;
import com.smile.start.model.project.ApplyHistory;
import com.smile.start.model.project.ApplyHistoryParam;
import com.smile.start.model.project.Audit;
import com.smile.start.model.project.AuditParam;
import com.smile.start.service.project.ApplyHistoryService;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;


/**
 * 项目申请历史
 * @author Joseph
 * @version v1.0 2019/5/3 10:21, ApplyHistoryController.java
 * @since 1.8
 */
@Controller
@RequestMapping("/applyHistory")
public class ApplyHistoryController extends BaseController {

    @Resource
    private ApplyHistoryService applyHistoryService;

    @GetMapping
    public String index() {
        return "project/applyHistory";
    }

    /**
     * 分页查询
     * @param query
     * @return
     */
    @PostMapping("/query")
    @ResponseBody
    public PageInfo<ApplyHistory> query(@RequestBody PageRequest<ApplyHistoryParam> query) {
        LoggerUtils.info(logger, "查询参数={}", FastJsonUtils.toJSONString(query));
        return applyHistoryService.query(query);
    }
}