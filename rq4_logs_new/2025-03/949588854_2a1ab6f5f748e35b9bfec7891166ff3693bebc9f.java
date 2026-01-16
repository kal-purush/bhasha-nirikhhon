package com.soft.technology.transactions_management.application.service;

import com.soft.technology.transactions_management.domain.model.EmpresaDTO;
import com.soft.technology.transactions_management.domain.port.EmpresaRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

@ExtendWith(MockitoExtension.class)
class DomainEmpresaServiceTest {

    @Mock
    EmpresaRepository empresaRepository;

    @Mock
    TransacionesService transacionesService;

    @InjectMocks
    DomainEmpresaService domainEmpresaService;

    @Test
    void when_repository_lastmonth_adherence_returns_ERROR_then_return_exception() {
        RuntimeException re = new RuntimeException();
        doThrow(re)
                .when(empresaRepository)
                .getEmpresasByLastMonthsOfAdherence();

        assertThatCode(() -> domainEmpresaService.getEmpresasByLastMonthsOfAdherence())
                .isEqualTo(re);
    }

    @Test
    void when_repository_lastmonth_adherence_returns_success_then_return_adherences() {
        EmpresaDTO empresa = new EmpresaDTO(1234L, "SUPER", new Date());
        Iterable<EmpresaDTO> listEmpresasExpected = Collections.singletonList(empresa);

        doReturn(listEmpresasExpected)
                .when(empresaRepository)
                .getEmpresasByLastMonthsOfAdherence();

        Iterable<EmpresaDTO> listEmpresasActual = domainEmpresaService.getEmpresasByLastMonthsOfAdherence();

        assertThat(listEmpresasActual).isEqualTo(listEmpresasExpected);
    }

    @Test
    void when_repository_transferences_lastmonth_returns_ERROR_then_return_exception() {
        RuntimeException re = new RuntimeException();
        doThrow(re)
                .when(transacionesService)
                .getEmpresasWithTransferencesByLastMonths();

        assertThatCode(() -> domainEmpresaService.getEmpresasWithTransferencesByLastMonths())
                .isEqualTo(re);
    }

    @Test
    void when_repository_transferences_lastmonth_returns_success_then_return_empresas() {
        EmpresaDTO empresaA = new EmpresaDTO(1234L, "SUPERA", new Date());
        EmpresaDTO empresaB = new EmpresaDTO(5678L, "SUPERB", new Date());
        Iterable<EmpresaDTO> listEmpresasExpected = Arrays.asList(empresaA,empresaB);
        List<Long> cuitsEmpresas = Arrays.asList(1234L, 5678L);

        doReturn(cuitsEmpresas)
                .when(transacionesService)
                .getEmpresasWithTransferencesByLastMonths();
        doReturn(listEmpresasExpected)
                .when(empresaRepository)
                .findByCuitContaining(cuitsEmpresas);

        Iterable<EmpresaDTO> listEmpresasActual = domainEmpresaService.getEmpresasWithTransferencesByLastMonths();

        assertThat(listEmpresasActual).isEqualTo(listEmpresasExpected);
    }

    @Test
    void when_repository_add_empresa_returns_ERROR_then_return_exception() {
        EmpresaDTO empresa = new EmpresaDTO(1234L, "SUPER", new Date());
        RuntimeException re = new RuntimeException();
        doThrow(re)
                .when(empresaRepository)
                .addEmpresa(empresa);

        assertThatCode(() -> domainEmpresaService.addEmpresa(empresa))
                .isEqualTo(re);
    }

    @Test
    void when_repository_add_empresa_returns_success_then_return_adherences() {
        EmpresaDTO empresaExpected = new EmpresaDTO(1234L, "SUPER", new Date());

        doReturn(empresaExpected)
                .when(empresaRepository)
                .addEmpresa(empresaExpected);

        EmpresaDTO empresasActual = domainEmpresaService.addEmpresa(empresaExpected);

        assertThat(empresasActual).isEqualTo(empresaExpected);
    }
}