package com.bolsadeideas.springboot.app.jpa.models.dao;

import com.bolsadeideas.springboot.app.jpa.models.entity.Cliente;

import javax.persistence.EntityManager;
import java.util.List;

public class ClienteDaoImpl implements ICliente{

    private EntityManager entityManager;  //Permite la amdinistracion de querys es el que se encarga de las operaciones de la DB

    @Override
    public List<Cliente> findAll() {
        return entityManager.createQuery("from cliente").getResultList();
    }
}