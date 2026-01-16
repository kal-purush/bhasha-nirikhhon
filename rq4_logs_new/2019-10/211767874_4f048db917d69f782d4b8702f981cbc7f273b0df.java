package com.aowin.controller;

import java.util.List;

import javax.annotation.Resource;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.aowin.model.Bicycle_catagory;
import com.aowin.model.Bicycle_pile;
import com.aowin.model.Bicycle_station;
import com.aowin.service.IBicycle_stationInformationService;

/**
 * 车点车辆信息查询
 * 
 * @author Peter
 *
 */
@RequestMapping("inform")
@RestController
public class Bicycle_stationInformationSearchController {
	@Resource
	private IBicycle_stationInformationService bicycle_stationInformationService;

	@RequestMapping("getBicycle_stationListByNameOrAdd.do")
	public List<Bicycle_station> getBicycle_stationListByNameOrAdd(@RequestBody Bicycle_station bicycle_station) {
		return bicycle_stationInformationService.getBicycle_stationListByNameOrAdd(bicycle_station);
	}

	/**
	 * 根据车点经纬度查询车桩集合
	 */
	@RequestMapping("getBicycle_pileListByLogAndLat.do")
	public List<Bicycle_pile> getBicycle_pileListByLogAndLat(Bicycle_station bicycle_station) {
		return bicycle_stationInformationService.getBicycle_pileListByLogAndLat(bicycle_station);
	}

	/**
	 * 根据车辆id查询车辆信息
	 * 
	 * @param bicycle_id
	 * @return
	 */
	@RequestMapping("getBicycle_catagoryById.do")
	public Bicycle_catagory getBicycle_catagoryById(Integer bicycle_id) {
		return bicycle_stationInformationService.getBicycle_catagoryById(bicycle_id);

	}
}