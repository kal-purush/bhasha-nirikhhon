package org.example.model;

import org.example.util.Index;

public class Result {
    //合并文档及排序用
    private Integer id; //DocInfo的id，文档合并时，文档身份标识
    //int -->初始化时为0
    private int weight;//权重，同一个文档合并后，权重相加在排序
    //返回给前端用
    private String title; //对应文档标题 DocInfo
    private String url; //对应DocInfo的url
    private String desc; //对应DocInfo的content（超长时，截取指定长度）

    @Override
    public String toString() {
        return "Result{" +
                "id=" + id +
                ", weight=" + weight +
                ", title='" + title + '\'' +
                ", url='" + url + '\'' +
                ", desc='" + desc + '\'' +
                '}';
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }
}