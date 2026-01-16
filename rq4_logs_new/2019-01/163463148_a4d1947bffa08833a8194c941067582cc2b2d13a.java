package cn.edu.tit.idao;

import java.util.List;

import org.springframework.stereotype.Component;

import cn.edu.tit.bean.Accessory;
import cn.edu.tit.bean.Student;
import cn.edu.tit.bean.Task;
import cn.edu.tit.bean.Teacher;
import cn.edu.tit.bean.VirtualClass;

@Component
public interface IStudentDao {
	public Student studentLoginByEmployeeNum(String employeeNum);	//通过学号登录学生
	public Student studentLoginByTelephone(String telephone);	//通过手机号登录学生
	public Student studentLoginByEmail(String email);	//通过邮件登录学生
	
	public List<String> studentVirtualClassId(String classnum);		//查询学生所在虚拟班级班级号列表，即班级列表	
	public List<VirtualClass> studentVirtualClass(List<String> virtualClassNums);	//根据虚拟班号列表查询所有虚拟班级实体
	
	public void upTask();	//提交任务
	public void upAccessory(String taskId,String student);
	public List<String> searchTaskId(String virtualClassNum);//查找班级对应的taskid号
	public List<Task> TaskList(List<String> taskIds);	//显示所有任务列表
	public Task searchTask(String taskId);	//查看单个任务详情
	public Accessory searchAccessory(String taskId);	//查询任务附件
	
	public List<String> searchRealClassNum(String virtualClassNum);		//查询虚拟班级对应的自然班列表模块
	public Student searchStudent(String studentId);		//查看学生信息模块
	
	
}