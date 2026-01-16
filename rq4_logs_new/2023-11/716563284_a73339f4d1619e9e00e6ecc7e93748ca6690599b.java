package com.botdatamessage;

import com.botdatamessage.model.Domain;
import com.botdatamessage.repository.DomainRepository;
import com.botdatamessage.service.impl.DomainServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class DomainServiceImplTest {
    @Mock
    private DomainRepository domainRepository;
    @InjectMocks
    private DomainServiceImpl domainService = new DomainServiceImpl(domainRepository);

@Test
    void addDomainTest() {
    Domain domain = new Domain("11",1,1,1,1,1,1,"22",23, LocalDate.of(2023, 02, 10),true,true,true);
    when(domainRepository.save(any())).thenReturn(domain);
    domainService.addDomain(domain);
    verify(domainRepository, times(1)).save(any());
    }
    @Test
    void clearDomainTest() {
         doNothing().when(domainRepository).deleteAll();
        domainService.clearDomain();
        verify(domainRepository, times(1)).deleteAll();
    }
}