package com.levchenko.hw25.model;

import com.sun.istack.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import javax.persistence.*;
import java.sql.Date;

@Entity(name = "tickets")
public class Ticket {
    @ManyToOne
    private com.levchenko.hw25.model.Session session;
    @Id
    @GeneratedValue(generator = "increment")
    private int id;
    @ManyToOne
    private User user;
    @Column
    private Date dateBuy;
    @Column
    private int seatNum;


}