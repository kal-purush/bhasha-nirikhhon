package com.example.backend.services.Impl;

import com.example.backend.dtos.PaymentDTO;
import com.example.backend.models.Fee;
import com.example.backend.models.Resident;
import com.example.backend.services.PaymentService;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class PaymentServiceImpl implements PaymentService {
    @Override
    public Page<PaymentDTO> findAll(Pageable pageable) {
        return null;
    }

    @Override
    public Page<PaymentDTO> findByUserId(Long userId, Pageable pageable) {
        return null;
    }

    @Override
    public PaymentDTO save(JsonNode data, Long id) {
        return null;
    }

    @Override
    public PaymentDTO update(JsonNode data, Long id) {
        return null;
    }


    @Override
    public void save(Fee fee, Resident Resident) {

    }

}