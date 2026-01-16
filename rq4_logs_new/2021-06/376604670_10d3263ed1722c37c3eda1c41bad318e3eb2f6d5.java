package ru.alexkm07.techjparser.model.event;

import lombok.Data;
import ru.alexkm07.techjparser.model.fildClass.*;
import ru.alexkm07.techjparser.model.fildClass.Process;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Data
public class TimeoutEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "mssql_seq")
    @SequenceGenerator(name = "mssql_seq", sequenceName = "mssql_seq")
    private Long id;
    private LocalDateTime dateEvent;
    private Long duration;
    @ManyToOne
    private ProcessName processName;
    @ManyToOne
    private Process process;
    private String clientID;
    @ManyToOne
    private ApplicationName applicationName;
    @ManyToOne
    private ComputerName computerName;
    private Long connectID;
    private Long sessionID;
    @ManyToOne
    private UserEvent user;
    private Long transId;
    private Long waitConnections;
    @Column(length = 500000)
    private String contextText;
}