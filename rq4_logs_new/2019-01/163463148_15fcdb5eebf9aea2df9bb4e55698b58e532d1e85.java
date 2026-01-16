package cn.edu.tit.controller;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import cn.edu.tit.bean.Course;
import cn.edu.tit.bean.Teacher;
import cn.edu.tit.iservice.ITeacherService;

@RequestMapping("/main")
@Controller
public class MainController {
	@Autowired
	public ITeacherService teacherService;
	
	/**
	 * 跳转到主页
	 * @param request
	 * @return
	 */
	@RequestMapping(value="toMain")
	public ModelAndView toMain(HttpServletRequest request){
		ModelAndView mv = new ModelAndView();
		try {
			//按时间查询前12个课程信息
			List<Course> courseList = teacherService.getCourseByLimit();
			//获取创课教师
			List<String> teacherNameList = new ArrayList<>();
			for(Course course : courseList){
				teacherNameList.add(teacherService.getTeacherNameById(course.getPublisherId()));
			}
			mv.addObject("courseList",courseList);
			mv.addObject("teacherNameList",teacherNameList);
			mv.setViewName("/jsp/main");
			mv.addObject("teacher",request.getSession().getAttribute("teacher"));
			mv.addObject("student",request.getSession().getAttribute("student"));
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			return null;
		}
		return mv;
	}
}