package com.diluv.confluencia.database.record;

import javax.persistence.Column;
import javax.persistence.Id;

import java.io.Serializable;
import java.util.Objects;

public class ImagesEntityPK implements Serializable {

    @Id
    @Column(name = "id")
    private String id;

    @Id
    @Column(name = "ext")
    private String ext;

    @Id
    @Column(name = "width")
    private int width;

    public String getId () {

        return this.id;
    }

    public void setId (String id) {

        this.id = id;
    }

    public String getExt () {

        return this.ext;
    }

    public void setExt (String ext) {

        this.ext = ext;
    }

    public int getWidth () {

        return this.width;
    }

    public void setWidth (int width) {

        this.width = width;
    }

    @Override
    public boolean equals (Object o) {

        if (this == o) return true;
        if (!(o instanceof ImagesEntityPK)) return false;
        ImagesEntityPK that = (ImagesEntityPK) o;
        return Objects.equals(getId(), that.getId()) &&
            Objects.equals(getExt(), that.getExt()) &&
            Objects.equals(getWidth(), that.getWidth());
    }

    @Override
    public int hashCode () {

        return Objects.hash(getId(), getExt(), getWidth());
    }
}