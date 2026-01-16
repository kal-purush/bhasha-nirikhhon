package org.andy.kmap.enums;

/**
 * Created by lichao on 16/4/19.
 */
public enum RolesEnum {

    NORMAL_USER("普通用户",0),
    ADMMIN_USER("管理员",1);

    private final String roleName;
    private final int roleId;

    RolesEnum(String roleName,int roleId){
        this.roleId=roleId;
        this.roleName=roleName;
    }

    public String getRoleName(){
        return roleName;
    }

    public int getRoleId(){
        return roleId;
    }

}