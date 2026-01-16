package gp.lcw.sd.by.g3p.extension.dao;

import gp.lcw.sd.by.g3p.base.domain.BaseEntity;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Entity
@Table(name = "news")
@Setter
@Getter
public class news  {


    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long pageId;

    @Column
     String newsPageVideoUrl;

    @Column
    ArrayList<String> picsNewsMsg = new ArrayList<String>();


   //  Set<String>picsNewsMsg=new HashSet<String>();

    @OneToMany(mappedBy = "news")
    Set<newsMessage>newsMessageSet=new HashSet<newsMessage>();



}