package cn.ssm.controller;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import cn.ssm.po.Student;
import cn.ssm.service.StudentService;

@Controller
@RequestMapping(value="/student")
public class StudentController {
	
	@Autowired
	private StudentService service;
	
	@RequestMapping(value="/getAllStudent", method=RequestMethod.GET)
	public String getAllStudent(Model model) throws Exception {
				
		List<Student> datas = service.getAllStudent();
		
		model.addAttribute("studentList", datas);
		return "studentList";
	}
	
	@RequestMapping(value="/deleteStudent", method=RequestMethod.GET)
	public String deleteStudent(String sid) throws Exception {
		
		service.deleteStudentById(sid);
		
		return "forward:getAllStudent";
	}
	
	@RequestMapping(value="/modifyStudent", method=RequestMethod.GET)
	public String modifyStudent(Model model, String sid) throws Exception {
		
		Student student = service.getStudentById(sid);
		model.addAttribute("stuInfo", student);
		
		return "studentModify";
	}
	
	@RequestMapping(value="/updateStudent", method=RequestMethod.POST)
	public String updateStudent(Student stu) throws Exception {
		
		service.updateStudent(stu);
		return "redirect:getAllStudent";
	}
	
	@RequestMapping(value="/addStudent", method=RequestMethod.POST)
	public String addStudent(@ModelAttribute Student stu, Model model) throws Exception {
		
		boolean flag = service.addStudent(stu);
		
		if(!flag) {
			model.addAttribute("repeatAddMessage", "该学生信息已存在，不能重复添加！");
			return "studentAdd";
		} 
		return "redirect:getAllStudent";
	}
}