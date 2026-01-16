package gov.hygs.htgl.controller;

import gov.hygs.htgl.entity.Tkfl;
import gov.hygs.htgl.entity.Tmly;
import gov.hygs.htgl.service.TkcspzService;

import java.util.Collection;
import java.util.List;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.bstek.dorado.annotation.DataProvider;
import com.bstek.dorado.annotation.DataResolver;

@Component
public class TkcspzController {
	@Resource
	TkcspzService tkcspzService;
	
	@DataProvider
	public Collection<Tkfl> getTkflRoot(){
		return tkcspzService.getTkflRoot();
	}
	
	@DataProvider
	public Collection<Tkfl> getCurrentTkflById(String id){
		return tkcspzService.getCurrentTkflById(id);
	}
	
	@Transactional
	@DataResolver
	public void updateTkfl(List<Tkfl> list){
		tkcspzService.updateTkfl(list);
	}
	/*
	@DataResolver
	public void updateTkfl(List<Tkfl> list){
		for(Tkfl tkfl : list){
			System.out.println("tkfl state is = "+EntityUtils.getState(tkfl));
			List<Tkfl> childs = (List<Tkfl>) tkfl.getChild();
			if(childs != null){
				for(Tkfl child : childs){
					System.out.println("child state is = "+EntityUtils.getState(child));
				}
			}
		}
	}
	*/
	
	@DataProvider
	public Collection<Tmly> getTmlyInfo(){
		return tkcspzService.getTmlyInfo();
	}
	
	@DataResolver
	public void updateTmly(List<Tmly> tmlys){
		tkcspzService.updateTmly(tmlys);
	}
}