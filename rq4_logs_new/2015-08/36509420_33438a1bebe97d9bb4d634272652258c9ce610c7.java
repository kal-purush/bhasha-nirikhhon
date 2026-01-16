package com.oc.action;

import org.apache.log4j.Logger;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.springframework.beans.factory.annotation.Autowired;

import com.oc.action.base.BaseAction;
import com.oc.dto.Talent;
import com.oc.service.BaseServiceI;
import com.oc.service.TalentServiceI;
import com.oc.utils.system.Json;

@Namespace("/")
@Action(value = "talentAction", results = {
		//@Result(name = "roleAdd", location = "/admin/roleAdd.jsp"), 
		//@Result(name = "roleEdit", location = "/admin/roleEdit.jsp") 
		})
@SuppressWarnings("all")
public class TalentAction extends BaseAction{
	private static final Logger logger = Logger.getLogger(ResumeAction.class);
	//简历id，从前台页面传递过来，用来查询简历其他信息
	private String talentIds;//批量删除简历，前台传递过来的ids
	Talent talent = new Talent();//简历基本信息实体
	
	

	@Autowired
	private TalentServiceI talentService;
	
	public String saveOrUpdateTalent(){
		Json j = new Json();
		try {
			boolean result = talentService.saveOrUpdateTalent(talent);
			if(result){
				j.setSuccess(true);
				j.setMsg("人才信息保存成功");
				j.setObj(talent.getId());//新增后的实体id
			}else{
				j.setSuccess(false);
				j.setMsg("数据库中已经存在该人才信息，不能再次添加");
			}
		} catch (Exception e) {
			j.setSuccess(false);
			j.setMsg("人才信息保存失败");
		}
		writeJson(j);
		return null;
	}
	
	/**
	 * 查询满足条件的简历信息并进行展示
	 * */
	public void datagrid(){
		
		super.writeJson(talentService.datagrid(talent));
	}
	
	public void deleteTalent() {
		talentService.deleteTalent(talentIds);
		Json j = new Json();
		j.setSuccess(true);
		j.setMsg("删除成功");
		writeJson(j);
	}

	@Override
	protected BaseServiceI getService() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Object getModel() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String getTalentIds() {
		return talentIds;
	}

	public void setTalentIds(String talentIds) {
		this.talentIds = talentIds;
	}

	public Talent getTalent() {
		return talent;
	}

	public void setTalent(Talent talent) {
		this.talent = talent;
	}

	public TalentServiceI getTalentService() {
		return talentService;
	}

	public void setTalentService(TalentServiceI talentService) {
		this.talentService = talentService;
	}
}