package org.jeecg.modules.ws.patrol.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.jeecg.common.api.vo.Result;
import org.jeecg.common.aspect.annotation.AutoLog;
import org.jeecg.common.system.query.QueryGenerator;
import org.jeecg.common.util.DateUtils;
import org.jeecg.common.util.oConvertUtils;
import org.jeecg.modules.demo.test.entity.JeecgDemo;
import org.jeecg.modules.ws.ass.entity.AssInfo;
import org.jeecg.modules.ws.patrol.entity.DutyInput;
import org.jeecg.modules.ws.patrol.service.DutyService;
import org.jeecgframework.poi.excel.ExcelImportUtil;
import org.jeecgframework.poi.excel.def.NormalExcelConstants;
import org.jeecgframework.poi.excel.entity.ExportParams;
import org.jeecgframework.poi.excel.entity.ImportParams;
import org.jeecgframework.poi.excel.view.JeecgEntityExcelView;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;
@RestController
@RequestMapping("/ws/duty")
@Slf4j
public class DutyController {
    @Autowired
    private DutyService dutyService;
    /**
     * 分页列表查询
     *
     * @param dutyInput
     * @param pageNo
     * @param pageSize
     * @param req
     * @return
     */

    @ApiOperation(value = "获取Demo数据列表", notes = "获取所有Demo数据列表", produces = "application/json")
    @GetMapping(value = "/list")
    public Result<IPage<DutyInput>> list(DutyInput dutyInput, @RequestParam(name = "pageNo", defaultValue = "1") Integer pageNo, @RequestParam(name = "pageSize", defaultValue = "10") Integer pageSize,
                                         HttpServletRequest req) {

        Result<IPage<DutyInput>> result = new Result<>();
        QueryWrapper<DutyInput> queryWrapper = QueryGenerator.initQueryWrapper(dutyInput, req.getParameterMap());
        Page<DutyInput> page = new Page<>(pageNo, pageSize);
        IPage<DutyInput> pageList = dutyService.page(page, queryWrapper);
        result.setSuccess(true);
        result.setResult(pageList);
        return result;
    }

    /**
     * 添加
     *
     * @param dutyInput
     * @return
     */
    @PostMapping(value = "/add")
    @AutoLog(value = "添加测试DEMO")
    public Result<DutyInput> add(@RequestBody DutyInput dutyInput) {
        Result<DutyInput> result = new Result<>();
        try {
            dutyService.save(dutyInput);
            result.success("添加成功！");
        } catch (Exception e) {
            e.printStackTrace();
            log.info(e.getMessage());
            result.error500("操作失败");
        }
        return result;
    }

    /**
     * 编辑
     *
     * @param dutyInput
     * @return
     */
    @PutMapping(value = "/edit")
    public Result<AssInfo> eidt(@RequestBody DutyInput dutyInput) {
        Result<AssInfo> result = new Result<>();
        DutyInput AssInfoEntity = dutyService.getById(dutyInput.getId());
        if (AssInfoEntity == null) {
            result.error500("未找到对应实体");
        } else {
            boolean ok = dutyService.updateById(dutyInput);
            // TODO 返回false说明什么？
            if (ok) {
                result.success("修改成功!");
            }
        }

        return result;
    }

    /**
     * 通过id删除
     *
     * @param id
     * @return
     */
    @AutoLog(value = "删除测试DEMO")
    @DeleteMapping(value = "/delete")
    public Result<DutyInput> delete(@RequestParam(name = "id", required = true) String id) {
        Result<DutyInput> result = new Result<>();
        DutyInput dutyInput = dutyService.getById(id);
        if (dutyInput == null) {
            result.error500("未找到对应实体");
        } else {
            boolean ok = dutyService.removeById(id);
            if (ok) {
                result.success("删除成功!");
            }
        }

        return result;
    }

    /**
     * 批量删除
     *
     * @param ids
     * @return
     */
    @DeleteMapping(value = "/deleteBatch")
    public Result<DutyInput> deleteBatch(@RequestParam(name = "ids", required = true) String ids) {
        Result<DutyInput> result = new Result<>();
        if (ids == null || "".equals(ids.trim())) {
            result.error500("参数不识别！");
        } else {
            this.dutyService.removeByIds(Arrays.asList(ids.split(",")));
            result.success("删除成功!");
        }
        return result;
    }

    /**
     * 通过id查询
     *
     * @param id
     * @return
     */
    @ApiOperation(value = "获取Demo信息", tags = { "获取Demo信息" }, notes = "注意问题点")
    @GetMapping(value = "/queryById")
    public Result<DutyInput> queryById(@ApiParam(name = "id", value = "示例id", required = true) @RequestParam(name = "id", required = true) String id) {
        Result<DutyInput> result = new Result<>();
        DutyInput dutyInput = dutyService.getById(id);
        if (dutyInput == null) {
            result.error500("未找到对应实体");
        } else {
            result.setResult(dutyInput);
            result.setSuccess(true);
        }
        return result;
    }

    // ==========================================动态表单 JSON接收测试===========================================//
    @PostMapping(value = "/testOnlineAdd")
    public Result<JeecgDemo> testOnlineAdd(@RequestBody JSONObject json) {
        Result<JeecgDemo> result = new Result<JeecgDemo>();
        log.info(json.toJSONString());
        result.success("添加成功！");
        return result;
    }

}