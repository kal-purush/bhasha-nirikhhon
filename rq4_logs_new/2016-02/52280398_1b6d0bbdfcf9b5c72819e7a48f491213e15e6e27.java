package com.buaa;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import com.buaa.student.service.IStudent;
import com.buaa.student.vo.Student;

/** 
* @ProjectName MybatisPagination
* @PackageName com.buaa
* @ClassName Test
* @Description TODO
* @Author 刘吉超
* @Date 2016-02-18 20:39:46
*/
public class Test {
	public static void main(String[] args) throws IOException {
		String resource = "config.xml";
		InputStream inputStream = Resources.getResourceAsStream(resource);
		SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
		SqlSession session = sqlSessionFactory.openSession();
		try {
			IStudent studentDao = session.getMapper(IStudent.class);
			
			Student student = new Student();
			
			// 打开分页功能
			student.setOpenPage(true);
			
			// 设置分页参数
			student.setRowsPerPage(2);
			student.setCurrentPage(2);
			
			List<Student> list = studentDao.selectStudent(student);
			
			for(Student s : list){
				System.out.println(s.getId()+"----"+s.getName());
			}
	        
			session.commit();
		} finally {
		  session.close();
		}
	}
}