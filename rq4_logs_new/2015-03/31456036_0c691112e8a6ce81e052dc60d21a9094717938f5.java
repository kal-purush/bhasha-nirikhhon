package com.carwash.dao;

import java.util.List;

import com.carwash.model.Area;
import com.carwash.model.City;

/**
 * Meta Data Access Object (GenericDao) interface.
 * 
 * @author mathi
 */

public interface MetaDataDao extends GenericDao<Area, String> {

	/**
	 * auto complete area Suggestions
	 * 
	 * @param query
	 * @return
	 */
	List<City> citySuggestions(String query);

	/**
	 * get area by id
	 * 
	 * @param id
	 * @return
	 */
	Area getArea(String id);

	/**
	 * get area by name
	 * 
	 * @param areaName
	 * @return
	 */
	Area getAreaByName(String areaName);

	/**
	 * auto complete city Suggestions
	 * 
	 * @param query
	 * @return
	 */
	List<Area> areaSuggestions(String query, String cityId);

	/**
	 * get city by id
	 * 
	 * @param id
	 * @return
	 */
	City getCity(String id);

	/**
	 * get city by name
	 * 
	 * @param cityName
	 * @return
	 */
	City getCityByName(String cityName);

}