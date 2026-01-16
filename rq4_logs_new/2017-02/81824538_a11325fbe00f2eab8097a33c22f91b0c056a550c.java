package cn.xingfire.applet.controller;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import cn.xingfire.applet.entity.response.AppInfo;
import cn.xingfire.applet.entity.response.Apps;
import cn.xingfire.applet.entity.response.ErrorInfo;
import cn.xingfire.applet.entity.response.HomePage;
import cn.xingfire.applet.pojo.App;
import cn.xingfire.applet.service.IAppService;
import cn.xingfire.applet.util.BusinessUtil;
import cn.xingfire.applet.util.CommonUtil;
import cn.xingfire.applet.util.ErrorInfoUtil;

@Controller
@RequestMapping("/app")
public class AppController {
	@Resource
	private IAppService appService;

	@ResponseBody
	@RequestMapping(value = "/getInfo", produces = "application/json;charset=UTF-8")
	public AppInfo getInfo(HttpServletRequest request) {
		App app = null;
		List<App> apps = new ArrayList<>();
		apps.add(this.appService.getInfo(request.getParameter("uuid")));
		BusinessUtil.operatorImages(apps);
		if (apps.size() > 0) {
			app = apps.get(0);
		}
		AppInfo appInfo = new AppInfo();
		appInfo.app = app;
		if (app == null) {
			appInfo.errorInfo = ErrorInfoUtil.NORESULT;
		}
		return appInfo;
	}
	
	@ResponseBody
	@RequestMapping(value = "/getAppsByKeyword", produces = "application/json;charset=UTF-8")
	public Apps getAppsByKeyword(HttpServletRequest request) {
		Apps appsR = new Apps();
		ErrorInfo error = null;
		try {
			String keyword = request.getParameter("keyword");
			if (StringUtils.isNotBlank(keyword)) {
				List<App> apps = this.appService.getAppsByKeyword(keyword);
				if (apps != null && apps.size() > 0) {
					appsR.apps = apps;
					appsR.total = apps.size() + "";
				} else {
					error = ErrorInfoUtil.NORESULT;
				}
			}
		} catch (Exception e) {
			error = ErrorInfoUtil.EXCEPTION;
		}
		appsR.errorInfo = error;
		return appsR;
	}

	@ResponseBody
	@RequestMapping(value = "/getApps", produces = "application/json;charset=UTF-8")
	public Apps getAppsByLevel(HttpServletRequest request) {
		int page = CommonUtil.toInt(request.getParameter("page"), 1);
		int size = CommonUtil.toInt(request.getParameter("size"), 15);
		int level = CommonUtil.toInt(request.getParameter("level"), 0);
		return this.appService.getAppsByLevel(level, page, size);
	}
	
	@ResponseBody
	@RequestMapping(value = "/getRecommendApps", produces = "application/json;charset=UTF-8")
	public Apps getRecommendApps() {
		return this.appService.getRecommendApps();
	}
	
	@ResponseBody
	@RequestMapping(value = "/getHomePage", produces = "application/json;charset=UTF-8")
	public HomePage getHomePage(HttpServletRequest request) {
		int page = CommonUtil.toInt(request.getParameter("page"), 1);
		int size = CommonUtil.toInt(request.getParameter("size"), 15);
		int level = CommonUtil.toInt(request.getParameter("level"), 0);
		HomePage hp = this.appService.getHomePageData(level, page, size);
		return hp;
	}
	
	@ResponseBody
	@RequestMapping(value = "/getLatestOnline", produces = "application/json;charset=UTF-8")
	public Apps getLatestOnline() {
		Apps appsRes = new Apps();
		ErrorInfo error = null;
		try {
			List<App> apps = this.appService.getLatestOnline();
			BusinessUtil.operatorImages(apps);
			if (apps != null && apps.size() > 0) {
				appsRes.apps = apps;
				appsRes.total = apps.size() + "";
			} else {
				appsRes.errorInfo = ErrorInfoUtil.NORESULT;
			}
		} catch (Exception e) {
			error = ErrorInfoUtil.EXCEPTION;
		}
		appsRes.errorInfo = error;
		return appsRes;
	}
	
	@ResponseBody
	@RequestMapping(value = "/getAppsByCategory", produces = "application/json;charset=UTF-8")
	public Apps getAppsByCategory(HttpServletRequest request) {
		return this.appService.getAppsByCategory(
				CommonUtil.toInt(
						this.appService.getInfo(request.getParameter("categoryId"))));
	}

}