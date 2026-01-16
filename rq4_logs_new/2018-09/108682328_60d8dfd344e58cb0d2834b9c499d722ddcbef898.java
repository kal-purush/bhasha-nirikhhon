package com.systelab.seed.util.pagination;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Page<T> implements Serializable {

    private long total;

    @XmlAnyElement(lax = true)
    private List<T> content;

    public Page() {
        this.content = new ArrayList();
        this.total = 0;
    }

    public Page(List<T> content, long total) {
        this.content = content;
        this.total = total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public void setContent(List<T> content) {
        this.content = content;
    }

    public long getTotal() {
        return total;
    }

    public List<T> getContent() {
        return content;
    }
}