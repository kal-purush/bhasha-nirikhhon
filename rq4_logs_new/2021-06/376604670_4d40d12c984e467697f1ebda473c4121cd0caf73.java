package ru.alexkm07.techjparser.repository.event;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.alexkm07.techjparser.model.event.TimeoutEvent;
import ru.alexkm07.techjparser.model.event.TlockEvent;
import ru.alexkm07.techjparser.model.fildClass.ProcessName;

import java.time.LocalDateTime;

public interface TlockEventRepository extends JpaRepository<TlockEvent, Long> {

    TlockEvent findByDateEventAndDurationAndProcessNameAndClientID(LocalDateTime date, Long duration, ProcessName processName, String clientId);

}