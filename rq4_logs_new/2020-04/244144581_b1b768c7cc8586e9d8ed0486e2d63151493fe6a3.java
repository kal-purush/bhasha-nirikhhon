package gp.lcw.sd.by.g3p.extension.dao.CommunicationSpace;

import com.fasterxml.jackson.annotation.JsonIgnore;
import gp.lcw.sd.by.g3p.base.domain.BaseEntity;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.util.List;

@Entity
@Setter
@Getter
@Table
public class messagePics extends BaseEntity  {


    @Lob
    @Basic(fetch = FetchType.LAZY)
    @Column(name = "picData", columnDefinition = "mediumblob")/*mediumblob一行可以存储16MB*/
    String picData;


    //@ManyToOne(cascade = CascadeType.ALL,fetch = FetchType.LAZY)
    //@JoinColumn(name = "id",insertable = false,updatable = false)/*  @JsonIgnore*/
    //message messageOne;

}