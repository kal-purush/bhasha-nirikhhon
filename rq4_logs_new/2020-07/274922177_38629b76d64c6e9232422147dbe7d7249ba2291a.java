package com.center.api.dto;

import com.center.api.base.BaseDto;

/**
 * @ClassName ResourceDto
 * @Description
 * @Author yutang
 * @Date 2020/7/5 11:14
 * @Version V1.0
 **/
public class ResourcesDto extends BaseDto {

    /**
     *  资源名称
     */
    private String name;

    /**
     * 资源类型
     */
    private String type;

    /**
     * 资源地址
     */
    private String url;

    /**
     * 权限
     */
    private String permission;

    /**
     * 资源父级id
     */
    private Long parentId;

    /**
     * 排序
     */
    private Integer sort;

    /**
     * 扩展字段
     */
    private Boolean external;

    /**
     * 是否可用
     */
    private Integer available;

    /**
     * 图标地址
     */
    private String icon;

    /**
     * 是否检查
     */
    private String checked;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getPermission() {
        return permission;
    }

    public void setPermission(String permission) {
        this.permission = permission;
    }

    public Long getParentId() {
        return parentId;
    }

    public void setParentId(Long parentId) {
        this.parentId = parentId;
    }

    public Integer getSort() {
        return sort;
    }

    public void setSort(Integer sort) {
        this.sort = sort;
    }

    public Boolean getExternal() {
        return external;
    }

    public void setExternal(Boolean external) {
        this.external = external;
    }

    public Integer getAvailable() {
        return available;
    }

    public void setAvailable(Integer available) {
        this.available = available;
    }

    public String getIcon() {
        return icon;
    }

    public void setIcon(String icon) {
        this.icon = icon;
    }

    public String getChecked() {
        return checked;
    }

    public void setChecked(String checked) {
        this.checked = checked;
    }
}