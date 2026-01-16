/**
 * 
 */
package cn.edu.tit.controller;

import java.io.File;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.ModelAndView;

import cn.edu.tit.bean.Student;
import cn.edu.tit.iservice.IStudentService;
import cn.edu.tit.iservice.ITeacherService;

/**
 * @author LiMing
 *
 */
@RequestMapping("/student")
@Controller
public class StudentController {

	@Autowired
	private IStudentService studentService;
	Student student ;

	/**
	 * 添加教师的方法  excel 相关的操作,将数据插入到数据库 
	 * 使用spring的MultipartFile上传文件
	 * */
	@RequestMapping(value="LoginStudent",method= {RequestMethod.POST})
	public ModelAndView LoginStudent(@RequestParam(value="studentId") String studentId,@RequestParam(value="studentPassword") String studentPassword) {			
		ModelAndView mv = new ModelAndView();
		System.out.println(studentId+"～～～～～～～～"+studentPassword+"!!!!!!!!!!!!!!");
		String readResult =null;
		try {
			student = studentService.studentLoginByEmployeeNum(studentId);
			if(studentPassword != student.getStudentPassword())
			{
				mv.addObject("readResult", readResult);//返回信息
				mv.setViewName("/error");//设置返回页面
			}
			else {
				mv.addObject("readResult", readResult);//返回信息
				mv.setViewName("/success");//设置返回页面
			}

		} catch (Exception e) {
			e.printStackTrace();
			mv.addObject("readResult", readResult);//返回信息
			mv.setViewName("/error");//设置返回页面
		}

		return mv;
	}


}