package com.bdg.converter.model_to_persistance;

import com.bdg.model.AddressMod;
import com.bdg.persistent.AddressPer;

public class ModToPerAddress extends ModToPer<AddressMod, AddressPer> {

    @Override
    public AddressPer getPersistentFromModel(AddressMod model) {
        if (model == null) {
            throw new NullPointerException("Passed null value as 'model': ");
        }

        AddressPer persistent = new AddressPer();
        persistent.setCountry(model.getCountry());
        persistent.setCity(model.getCity());
        return persistent;
    }
}