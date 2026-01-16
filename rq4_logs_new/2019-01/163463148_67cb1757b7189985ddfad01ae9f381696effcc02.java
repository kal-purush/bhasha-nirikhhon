package cn.edu.tit.idao;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import javax.print.attribute.standard.DateTimeAtCompleted;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;

import cn.edu.tit.bean.Accessory;
import cn.edu.tit.bean.Task;


@Component
public interface ITaskDao {
	public void createTask(Task task);	//创建任务
	public void stopTask(String taskId);	//截止任务
	public void restartTask(String taskId);	//重新启动任务，任务未过期
	public void restartTaskSetEndTime(@Param("taskId")String taskId,@Param("taskEndTime")Timestamp taskEndTime);	//重新启动任务，任务过期
	public void deleteTask(String taskId);	//删除任务
	public void mapClassTask(@Param("virtualClassNum")String virtualClassNum,@Param("taskId")String taskId);	//把任务映射到班级任务表中
	public void addAccessory(List<Accessory> accessoris);	//添加附件
	

}