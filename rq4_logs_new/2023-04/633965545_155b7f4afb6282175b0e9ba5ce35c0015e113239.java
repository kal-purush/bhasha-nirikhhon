package com.bdg.converter.model_to_persistance;

import com.bdg.model.CompanyMod;
import com.bdg.persistent.CompanyPer;

public class ModToPerCompany extends ModToPer<CompanyMod, CompanyPer> {

    @Override
    public CompanyPer getPersistentFromModel(CompanyMod model) {
        if (model == null) {
            throw new NullPointerException("Passed null value as 'model': ");
        }

        CompanyPer persistent = new CompanyPer();
        persistent.setName(model.getName());
        persistent.setFoundDate(model.getFoundDate());
        return persistent;
    }
}