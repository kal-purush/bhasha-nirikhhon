package com.luxoft.springdb.lab2.dao;

import java.util.List;

import com.luxoft.springdb.lab2.model.Country;

public interface CountryDao {

	public abstract void save(Country country);

	public abstract List<Country> getAllCountries();

	public abstract Country getCountryByName(String name);

}