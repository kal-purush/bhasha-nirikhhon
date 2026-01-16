package org.plan.managementfacade.model.enumModel;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = EnumSerializer.class)
public enum InfoState implements IEnum{

    NOTBIND(1, "未绑定"),
    BIND(2, "绑定");

    private Integer index;
    private String name;

    private InfoState (Integer index, String name) {
        this.index = index;
        this.name = name;
    }

    @Override
    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static String getName (Integer index) {
        for (InfoState infoState : values()) {
            if (infoState.index.equals(index)) {
                return infoState.name;
            }
        }
        return null;
    }

    public static Integer getIndex (String name) {
        for (InfoState infoState : values()) {
            if (infoState.name.equals(name)) {
                return infoState.index;
            }
        }
        return null;
    }
}