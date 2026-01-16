package net.teengamingnights.assemblymod.sql.datatypes.impl.date;

import net.teengamingnights.assemblymod.sql.datatypes.Datatype;

public class Date extends Datatype<java.sql.Date> {

    @Override
    public String getName() {
        return "DATE";
    }

    @Override
    public boolean takesParameters() {
        return false;
    }

    @Override
    public Class<java.sql.Date> getJavaType() {
        return java.sql.Date.class;
    }

    @Override
    public boolean matchesConstraints(java.sql.Date toTest) {
        return true;
    }
}