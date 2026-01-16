package com.autentia.catalogocursos.springbootmybatis.services.impl;

import com.autentia.catalogocursos.springbootmybatis.dto.CursoDTO;
import com.autentia.catalogocursos.springbootmybatis.mappers.CursoMapper;
import com.autentia.catalogocursos.springbootmybatis.models.Curso;
import com.autentia.catalogocursos.springbootmybatis.services.CursoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CursoServiceImpl implements CursoService {

    @Autowired
    CursoMapper cursoMapper;

    @Override
    public List<CursoDTO> getAll() {
        return cursoMapper.findAll();
    }

    @Override
    public List<CursoDTO> getAllCursosActivos() {
        return cursoMapper.findAllCursosActivos();
    }

    @Override
    public void insert(Curso curso) {
        cursoMapper.insert(curso);
    }
}