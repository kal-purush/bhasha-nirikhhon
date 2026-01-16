package com.hana.service;

import com.hana.data.dto.ServiceDto;
import com.hana.frame.HanaService;
import com.hana.repository.ServiceRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ServiceService implements HanaService<String, ServiceDto> {

    final ServiceRepository serviceRepository;

    @Override
    public int add(ServiceDto serviceDto) throws Exception {
        return serviceRepository.insert(serviceDto);
    }

    @Override
    public int del(String s) throws Exception {
        return serviceRepository.delete(s);
    }

    @Override
    public int modify(ServiceDto serviceDto) throws Exception {
        return serviceRepository.update(serviceDto);
    }

    @Override
    public ServiceDto get(String s) throws Exception {
        return serviceRepository.selectOne(s);
    }

    @Override
    public List<ServiceDto> get() throws Exception {
        return serviceRepository.select();
    }

    public List<ServiceDto> getByDetail(String detail, String option) throws Exception {
        List<ServiceDto> serviceDtoList = null;

        switch(option) {
            case "content" -> serviceDtoList = getByContent(detail);
            case "target" -> serviceDtoList = getByTarget(detail);
            case "location" -> serviceDtoList = getByTarget(detail);
        }
        return serviceDtoList;
    }

    private List<ServiceDto> getByTarget(String target) {
        return serviceRepository.selectByTarget(target);
    }

    private List<ServiceDto> getByContent(String content) {
        return serviceRepository.selectByContent(content);
    }
}