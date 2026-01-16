/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package pe.edu.pucp.gdptalento.eventos.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import pe.edu.pucp.gdptalento.eventos.dao.AsistenciaDAO;
import pe.edu.pucp.gdptalento.eventos.model.Asistencia;
import pe.edu.pucp.gdptalento.eventos.model.EstadoAsistencia;
import pe.edu.pucp.gdptalento.eventos.model.EstadoEvento;
import pe.edu.pucp.gdptalento.eventos.model.Evento;
import pe.edu.pucp.gdptalento.eventos.model.TipoEvento;
import pe.edu.pucp.gdptalento.miembros.model.EstadoMiembro;
import pe.edu.pucp.gdptalento.miembros.model.Staff;
import pucp.edu.pe.gdptalento.config.DBManager;

/**
 *
 * @author Dayana
 */
public class AsistenciaMySQL implements AsistenciaDAO{
    
    private Connection con;
    private Statement st;
    private ResultSet rs;
    
    @Override
    public int insertar(Asistencia asistencia) {
        int resultado=0;
        try{
            DBManager db = new DBManager();
            con = db.getConnection();
            
            String sql= "INSERT INTO Asistencia (id_evento, id_participante, asistencia)"
                    + "VALUES ('" + asistencia.getEvento().getId() +
                    "','" + asistencia.getParticipante().getId()+"')";
            System.out.println(sql);
            st = con.createStatement();
            resultado = st.executeUpdate(sql); 
        }catch(SQLException ex){
            System.out.println(ex.getMessage());
        }finally{
            try{con.close();}catch(SQLException ex){System.out.println(ex.getMessage());}
        }
        return resultado;
    }

    @Override
    public int modificar(Asistencia asistenciaD) {
       int resultado=0;
        try{
            DBManager db = new DBManager();
            con = db.getConnection();
            String sql= "UPDATE Asistencia SET asistencia = '"
                    + asistenciaD.getAsistencia() + "' WHERE"+  " id_evento = "+ asistenciaD.getEvento().getId()
                    + " and id_participante = " + asistenciaD.getParticipante().getId();
            st = con.createStatement();
            resultado = st.executeUpdate(sql); 
            System.out.println("Se ha actualizado la asistencia");
        }catch(SQLException ex){
            System.out.println(ex.getMessage());
        }finally{
            try{con.close();}catch(SQLException ex){System.out.println(ex.getMessage());}
        }
        return resultado;
    }

    @Override
    public ArrayList<Asistencia> listarTodas() {
        ArrayList<Asistencia> asistencias = new ArrayList<>();
        try{
            DBManager db = new DBManager();
            con = db.getConnection();
            
            String sql = "SELECT s.id_staff, m.nombre, m.telefono, s.area, a.asistencia, "
                    + "a.id_evento, e.fecha, e.tipoEvento, e.estadoEvento"
                    + "FROM Asistencia a, Staff s, MiembroPUCP m, Evento e"
                    + "WHERE a.id_participante = s.id_staff"
                    + "AND s.id_Staff = m.id_miembro_pucp"
                    + "AND a.id_evento = e.id_evento"
                    + "AND a.asistencia = 'ASISTIO'"
                    + "AND e.estadoEvento = 'APROBADO'"
                    + "AND e.tipoEvento = 'REUNION'"
                    + "AND s.estado = 'ACTIVO'";
            st = con.createStatement();
            rs = st.executeQuery(sql);
            while(rs.next()){
                Asistencia asistenciaD = new Asistencia();
                Evento eventoD = new Evento();
                Staff participante = new Staff();
                
                eventoD.setId(rs.getInt("id_evento"));
                eventoD.setFecha(rs.getDate("fecha").toLocalDate());
                eventoD.setEstadoEvento(EstadoEvento.APROBADO);
                eventoD.setTipoEvento(TipoEvento.REUNION);
                
                participante.setId(rs.getInt("id_staff"));
                participante.setNombre(rs.getString("nombre"));
                participante.setTelefono(rs.getInt("telefono"));
                participante.setEstado(EstadoMiembro.ACTIVO);
               
                
                asistenciaD.setAsistencia(EstadoAsistencia.ASISTIO);
                asistenciaD.setEvento(eventoD);
                asistenciaD.setParticipante(participante);
                asistencias.add(asistenciaD);
            }
        }catch(SQLException ex){
            System.out.println(ex.getMessage());
        }finally{
            try{rs.close();}catch(SQLException ex){System.out.println(ex.getMessage());}
            try{con.close();}catch(SQLException ex){System.out.println(ex.getMessage());}
        }
        return asistencias;
    }
    
}