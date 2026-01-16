package dataBean.dubbo.productSys;

import dataBean.IDataBean;

import java.io.Serializable;

/**
 * Created by alex on 2017/6/2.
 */
public class MyRelateProjectTemplateDto implements Serializable , IDataBean{


    private String id;


    /**
     * 项目编号
     */
    private String projectCode;
    /**
     * 模板编号
     */
    private String templateCode;

    /**
     * 项目在模板中显示名称
     */
    private String projectDisplayName;

    public String getProjectCode() {
        return projectCode;
    }

    public void setProjectCode(String projectCode) {
        this.projectCode = projectCode;
    }

    public String getTemplateCode() {
        return templateCode;
    }

    public void setTemplateCode(String templateCode) {
        this.templateCode = templateCode;
    }

    public String getProjectDisplayName() {
        return projectDisplayName;
    }

    public void setProjectDisplayName(String projectDisplayName) {
        this.projectDisplayName = projectDisplayName;
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}