package com.soft.technology.transactions_management.infrastructure.adapter;

import com.soft.technology.transactions_management.domain.model.EmpresaDTO;
import com.soft.technology.transactions_management.domain.port.EmpresaRepository;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;


@RunWith(SpringRunner.class)
@SpringBootTest
class EmpresaRepositoryH2Test {

    @Autowired
    private EmpresaRepository empresaRepository;

    @Test
    void when_repository_lastmonth_adherence_returns_then_return_success() {
        EmpresaDTO empresa = new EmpresaDTO(1234L, "SUPER", new Date());
        empresaRepository.addEmpresa(empresa);

        Iterable<EmpresaDTO> listEmpresas = empresaRepository.getEmpresasByLastMonthsOfAdherence();

        assertThat(listEmpresas).hasSize(3);
        assertThat(listEmpresas.iterator().next().getCuit()).isEqualTo(1234L);
        Assert.assertNotNull(listEmpresas.iterator().next());
    }

    @Test
    void when_repository_find_cuit_then_return_success() {
        EmpresaDTO empresa = new EmpresaDTO(1234L, "SUPER", new Date());
        List<Long> cuitsEmpresas = Arrays.asList(1234L);
        empresaRepository.addEmpresa(empresa);

        Iterable<EmpresaDTO> listEmpresas = empresaRepository.findByCuitContaining(cuitsEmpresas);

        assertThat(listEmpresas).hasSize(1);
        assertThat(listEmpresas.iterator().next().getCuit()).isEqualTo(1234L);
        Assert.assertNotNull(listEmpresas.iterator().next());
    }

    @Test
    void when_repository_save_empresa_then_return_success() {
        EmpresaDTO empresaExpected = new EmpresaDTO(1234L, "SUPER", new Date());

        EmpresaDTO empresaActual = empresaRepository.addEmpresa(empresaExpected);

        Assert.assertNotNull(empresaActual);
        assertThat(empresaActual.getCuit()).isEqualTo(empresaExpected.getCuit());
    }
}