package com.bdg.converter.model_to_persistance;

import com.bdg.model.AddressMod;
import com.bdg.model.PassengerMod;
import com.bdg.persistent.PassengerPer;
import com.bdg.validator.Validator;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

public class ModToPerPassenger extends ModToPer<PassengerMod, PassengerPer>{
    @Override
    public PassengerPer getPersistentFrom(PassengerMod model) {
        Validator.checkNull(model);
        ModToPerAddress temp = new ModToPerAddress();
        PassengerPer persistent = new PassengerPer();
        persistent.setName(model.getName());
        persistent.setPhone(model.getPhone());
        persistent.setAddress(temp.getPersistentFrom(model.getAddress()));
        return persistent;
    }

    @Override
    public Collection<PassengerPer> getPersistentListFrom(Collection<PassengerMod> modelList) {
        Validator.checkNull(modelList);
        Set<PassengerPer> passengersPerSet = new LinkedHashSet<>(modelList.size());
        for (PassengerMod tempPassengerMod : modelList){
            passengersPerSet.add(getPersistentFrom(tempPassengerMod));
        }
        return passengersPerSet;
    }
}