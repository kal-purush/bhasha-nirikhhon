package com.soft.technology.transactions_management.application.service;

import com.soft.technology.transactions_management.domain.port.TransaccionesRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

@ExtendWith(MockitoExtension.class)
class DomainTransaccionesServiceTest {

    @Mock
    private TransaccionesRepository transaccionesRepository;

    @InjectMocks
    private DomainTransaccionesService domainTransaccionesService;

    @Test
    void when_repository_transferences_lastmonth_returns_ERROR_then_return_exception() {
        RuntimeException re = new RuntimeException();
        doThrow(re)
                .when(transaccionesRepository)
                .getEmpresasWithTransferencesByLastMonths();

        assertThatCode(() -> domainTransaccionesService.getEmpresasWithTransferencesByLastMonths())
                .isEqualTo(re);
    }

    @Test
    void when_repository_transferences_lastmonth_returns_success_then_return_cuits() {
        List<Long> cuitsEmpresas = Arrays.asList(1234L, 5678L);

        doReturn(cuitsEmpresas)
                .when(transaccionesRepository)
                .getEmpresasWithTransferencesByLastMonths();

        List<Long> listEmpresasActual = domainTransaccionesService.getEmpresasWithTransferencesByLastMonths();

        assertThat(listEmpresasActual).isEqualTo(cuitsEmpresas);
    }
}