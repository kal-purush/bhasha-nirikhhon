package cn.xfakir.xmall.common.entity;

import org.springframework.security.core.GrantedAuthority;

import java.util.List;

public class XmRole implements GrantedAuthority {
    private String roleName;
    private List<XmAuthority> authorityList;

    public String getRoleName() {
        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    public List<XmAuthority> getAuthorityList() {
        return authorityList;
    }

    public void setAuthorityList(List<XmAuthority> authorityList) {
        this.authorityList = authorityList;
    }

    @Override
    public String getAuthority() {
        return "ROLE_" + this.roleName;
    }
}