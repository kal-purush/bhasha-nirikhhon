package com.soft.technology.transactions_management.application.service;

import com.soft.technology.transactions_management.domain.model.EmpresaDTO;
import com.soft.technology.transactions_management.domain.port.EmpresaRepository;

import java.util.List;

public class DomainEmpresaService implements EmpresaService {
    private final EmpresaRepository empresaRepository;
    private final TransacionesService transacionesService;

    public DomainEmpresaService(EmpresaRepository empresaRepository, TransacionesService transacionesService) {
        this.empresaRepository = empresaRepository;
        this.transacionesService = transacionesService;
    }

    @Override
    public Iterable<EmpresaDTO> getEmpresasByLastMonthsOfAdherence() {
        return this.empresaRepository.getEmpresasByLastMonthsOfAdherence();
    }

    @Override
    public Iterable<EmpresaDTO> getEmpresasWithTransferencesByLastMonths() {
        List<Long> cuitEmpresas = transacionesService.getEmpresasWithTransferencesByLastMonths();
        return this.empresaRepository.findByCuitContaining(cuitEmpresas);
    }

    @Override
    public EmpresaDTO addEmpresa(EmpresaDTO empresaDTO) {
        return this.empresaRepository.addEmpresa(empresaDTO);
    }
}