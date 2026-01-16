package gov.hygs.htgl.controller;

import gov.hygs.htgl.entity.Yxzsk;
import gov.hygs.htgl.entity.Zskly;
import gov.hygs.htgl.service.YxzskService;

import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.bstek.dorado.annotation.DataProvider;
import com.bstek.dorado.annotation.DataResolver;
import com.bstek.dorado.data.provider.Page;

@Component
public class YxzskController {
	@Resource
	YxzskService yxzskService;

	@DataProvider
	public void getYxzskInfo(Page<Yxzsk> page, Map<String, Object> param) {
		yxzskService.getYxzskInfo(page, param);
	}
	
	@DataProvider
	public List<Zskly> getZsklyInfoByZsklyId(Integer id){
		return yxzskService.getZsklyInfoByZsklyId(id);
	}

	@Transactional
	@DataResolver
	public void updateYxzsk(List<Yxzsk> yxzsks){
		yxzskService.updateYxzsk(yxzsks);
	}
}