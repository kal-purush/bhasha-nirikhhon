package gov.hygs.htgl.controller;

import gov.hygs.htgl.entity.Yxzsk;
import gov.hygs.htgl.entity.Zszsk;
import gov.hygs.htgl.service.ZszskService;

import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.bstek.dorado.annotation.DataProvider;
import com.bstek.dorado.annotation.DataResolver;
import com.bstek.dorado.data.provider.Page;

@Component
public class ZszskController {
	@Resource
	ZszskService zszskService;

	@DataProvider
	public void getZszskInfo(Page<Zszsk> page, Map<String, Object> param) {
		zszskService.getZszskInfo(page, param);
	}

	@DataProvider
	public void getYxzskInfo(Page<Yxzsk> page, Map<String, Object> param) {
		zszskService.getYxzskInfo(page, param);
	}

	@Transactional
	@DataResolver
	public void updateZszsk(List<Zszsk> zszsk) {
		zszskService.updateZszsk(zszsk);
	}
}