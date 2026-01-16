/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package controlador;

import dao.DAO;
import dao.DAOBIOSTAR;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import entidades.EventLog;

/**
 *
 * @author Aldo
 */
public class EventControlador extends Controlador<EventLog>{

    public EventControlador() {
        super(EventLog.class,new DAOBIOSTAR(EventLog.class));
    }
    
    public List<EventLog> buscarXEventosXID(String dni){
        int dniT = Integer.parseInt(dni);
        String jpql = "SELECT e FROM EventLog e WHERE CAST(e.nUserID AS INTEGER) = :id "
                + " AND e.nEventIdn IN ('41', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '55') ";
        Map<String, Object> mapa = new HashMap();
        mapa.put("id", dniT);
        
        return this.getDao().buscar(jpql, mapa); 
    }
    
    public List<EventLog> buscarXEventosXPagina(String dni, int desde, int tamanio){
        int dniT = Integer.parseInt(dni);
        String jpql = "SELECT e FROM EventLog e WHERE CAST(e.nUserID AS INTEGER) = :id "
                + " AND e.nEventIdn IN ('41', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '55') ";
        Map<String, Object> mapa = new HashMap();
        mapa.put("id", dniT);
        
        return this.getDao().buscar(jpql, mapa, desde, tamanio);
    }
    
    public int contarEventosXID(String dni){
        int dniT = Integer.parseInt(dni);
        String jpql = "SELECT COUNT(e.nEventLogIdn) FROM EventLog e WHERE CAST(e.nUserID AS INTEGER) = :id "
                + " AND e.nEventIdn IN ('41', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '55') ";
        Map<String, Object> mapa = new HashMap();
        mapa.put("id", dniT);
        
        return this.getDao().contar(jpql, mapa);
    }
}