package gp.lcw.sd.by.g3p.extension.dao;

import gp.lcw.sd.by.g3p.base.domain.BaseEntity;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table
@Setter
@Getter
public class newsVideos extends BaseEntity {

    @Column
    public String videoUrl;

    @Column
    public  String topFlag;

    @Column
    public int pointNumber;

    @Column
    public String videoName;

    @Column
    public String videoIntroduce;

}