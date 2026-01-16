package com.realestate.service;

import com.realestate.model.Country;
import com.realestate.model.Status;
import com.realestate.model.Type;

import java.util.List;

/**
 * Created by omererkan on 4/6/17.
 */
public class BannerImpl implements BannerService {
    List<Type> typeList;
    List<Status> statusList;
    List<Country> countryList;

    @Override
    public void addCountry(Country country) {

        if (country != null) {
            countryList.add(country);
        }
    }

    @Override
    public void addStatus(Status status) {


        if (status != null) {
            statusList.add(status);
        }

    }

    @Override
    public void addTypes(Type type) {

        if (type != null) {
            typeList.add(type);
        }
    }
}