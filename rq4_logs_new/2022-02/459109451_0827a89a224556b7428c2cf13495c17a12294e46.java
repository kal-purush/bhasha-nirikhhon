package com.cs.logfile.time.identifier.logfileidentifier.service;

import com.cs.logfile.time.identifier.logfileidentifier.configiration.LogFileIdentifireConfiguration;
import com.cs.logfile.time.identifier.logfileidentifier.entiry.EventStatus;
import com.cs.logfile.time.identifier.logfileidentifier.repository.EventStatusRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;


public class LogFileIdentifireServiceTest {

    @InjectMocks
    private LogFileIdentifireService logFileIdentifireService;
    @Mock
    DbOperationService dbOperationService;

    @Mock
    EventStatusRepository eventStatusRepository;

    @Mock
    LogFileIdentifireConfiguration logFileIdentifireConfiguration;

    @BeforeEach
    public void setup() {
        initMocks(this);
        logFileIdentifireConfiguration.limitTime=4;
        when(eventStatusRepository.save(any(EventStatus.class)))
                .thenAnswer((Answer<EventStatus>) invocation -> (EventStatus) invocation.getArguments()[0]);
    }

    @Test
    public void processFileTest(){
        doNothing().when(dbOperationService).saveInEventStatus(any());
        logFileIdentifireService.processFile("./src/main/resources/sampleLog.txt");
        verify(dbOperationService,times(2)).saveInEventStatus(any());
    }

}