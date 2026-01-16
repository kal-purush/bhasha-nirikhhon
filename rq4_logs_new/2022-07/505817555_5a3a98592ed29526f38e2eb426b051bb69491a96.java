package org.chuxue.application.dbms.appl.controller;

import java.util.List;

import org.chuxue.application.bean.manager.appl.SysApplTypeTabsInfo;
import org.chuxue.application.common.base.BaseController;
import org.chuxue.application.common.base.BaseControllerImpl;
import org.chuxue.application.common.base.BaseResult;
import org.chuxue.application.common.base.Pagination;
import org.chuxue.application.common.base.ResultUtil;
import org.chuxue.application.dbms.appl.service.SysApplTypeTabsInfoService;
import org.chuxue.application.dbms.appl.vo.SysApplTypeTabsInfoVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @文件名 SysApplTypeTabsInfoController.java
 * @包名 org.chuxue.application.bean.manager.appl.controller
 * @描述 controller层
 * @时间 2022年07月18日 16:07:49
 * @author
 * @版本 V1.0
 */
@RestController
@RequestMapping("/sysApplTypeTabsInfo")
public class SysApplTypeTabsInfoController extends BaseControllerImpl<SysApplTypeTabsInfo> implements BaseController<SysApplTypeTabsInfo> {

	@Autowired
	SysApplTypeTabsInfoService sysApplTypeTabsInfoService;

	@PostMapping("/findAllTablesCheck")
	public BaseResult<List<SysApplTypeTabsInfoVo>> findAllTablesCheck(@RequestBody SysApplTypeTabsInfoVo info) {
		try {
			List<SysApplTypeTabsInfoVo> list = sysApplTypeTabsInfoService.findAllTablesCheck(info);
			return ResultUtil.success(list);
		} catch (Exception e) {
			return ResultUtil.error(-1, e.getMessage());
		}
	}

	@PostMapping("/saveList")
	public BaseResult<List<SysApplTypeTabsInfoVo>> saveList(@RequestBody Pagination<SysApplTypeTabsInfo> vo) {
		try {
			sysApplTypeTabsInfoService.saveList(vo.getList());
			return ResultUtil.success();
		} catch (Exception e) {
			return ResultUtil.error(-1, e.getMessage());
		}
	}
}