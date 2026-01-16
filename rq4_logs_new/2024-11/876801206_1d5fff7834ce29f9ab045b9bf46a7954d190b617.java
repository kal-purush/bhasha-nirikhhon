package com.example.internaltools.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.internaltools.entity.DepartmentEntity;
import com.example.internaltools.repository.DepartmentRepository;

@Service
@Transactional
public class DepartmentService {
    
    @Autowired
    private DepartmentRepository departmentRepository; // DepartmentEntity用のリポジトリ

    public List<DepartmentEntity> getDepartment() {

        return departmentRepository.findAll();
    }
}