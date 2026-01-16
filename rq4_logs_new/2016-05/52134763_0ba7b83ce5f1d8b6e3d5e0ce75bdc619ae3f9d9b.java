package com.emi.nwodcombat.model.realm.wrappers;

import com.emi.nwodcombat.model.realm.Nature;

import io.realm.RealmObject;

/**
 * Created by Crux on 9/6/2016.
 * This class has a non-generic object for its 'value' field because an approach using a generic
 * RealmObject instead failed.
 */
public class NatureTrait extends RealmObject {
    private Long ordinal;
    private String type;
    private Nature nature;

    public Long getOrdinal() {
        return ordinal;
    }

    public void setOrdinal(Long ordinal) {
        this.ordinal = ordinal;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Nature getNature() {
        return nature;
    }

    public void setNature(Nature demeanor) {
        this.nature = demeanor;
    }
}