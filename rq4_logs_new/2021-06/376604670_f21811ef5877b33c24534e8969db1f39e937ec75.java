package ru.alexkm07.techjparser.model.event;

import lombok.Data;
import ru.alexkm07.techjparser.model.fildClass.Process;
import ru.alexkm07.techjparser.model.fildClass.*;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Data
public class ExcpEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "excp_seq")
    @SequenceGenerator(name = "excp_seq", sequenceName = "excp_seq")
    private Long id;
    private LocalDateTime dateEvent;
    @ManyToOne
    private ProcessName processName;
    @ManyToOne
    private Process process;
    private String clientID;
    @ManyToOne
    private ApplicationName applicationName;
    @ManyToOne
    private ComputerName computerName;
    private String exception;
    @Column(length = 5000)
    private String descr;
    private Integer hash;
}
package ru.alexkm07.techjparser.model.event;

import lombok.Data;
import ru.alexkm07.techjparser.model.fildClass.*;
import ru.alexkm07.techjparser.model.fildClass.Process;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Data
public class ExcpсntxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "excp_seq")
    @SequenceGenerator(name = "excp_seq", sequenceName = "excp_seq")
    private Long id;
    private LocalDateTime dateEvent;
    @ManyToOne
    private ComputerName clientComputerName;
    @ManyToOne
    private ComputerName serverComputerName;
    @ManyToOne
    private UserEvent userName;
    @Column(length = 500)
    private String ConnectString;

}