/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Controller;

import Model.*;
import View.InitialView;
import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;

/**
 *
 * @author fercon997
 */
public class PagoController {
    Nino nino;
    InitialView initialView;
    ArrayList<Pago> pagos;
    Inscripcion inscripcion;

    public PagoController(InitialView initialView, Nino nino) {
        this.nino = nino;
        this.initialView = initialView;
    }
    
    public void generarMenusalidad(){
        LocalDate localDate = LocalDate.now();
        int mes = localDate.getMonthValue();
        PagoDAOImpl bdPay = new PagoDAOImpl();
        InscripcionDAOImpl bdIns = new InscripcionDAOImpl();
        inscripcion = bdIns.getInsNino(nino);
        int mess = bdPay.getMesPago(mes, inscripcion.getConsecutivo());
        if (mess != mes){
            Pago pay = new Pago();
            pay.setConsecutivo(bdPay.getConsAnterior(inscripcion.getConsecutivo())+1);
            pay.setAno_inscripcion(inscripcion.getAno());
            pay.setCi_representante(nino.getCiRepresentante());
            pay.setConcepto("Pago por el mes de "+mesString(mes));
            pay.setCons_inscripcion(inscripcion.getConsecutivo());
            pay.setLetra(nino.getLetra());
            pay.setMes(mes);
            GuarderiaDAOImpl bdGuard = new GuarderiaDAOImpl();
            Guarderia guard = bdGuard.getDatosGuarderia(inscripcion.getRifGuarderia());
            if( (mes == 8) || (mes == 12))
              pay.setMonto( (float)guard.getCostoAgoDic());
            else 
              pay.setMonto( (float)guard.getCostoMensualidad());
            bdPay.insertPago(pay);        }
        
    }
    
    public String mesString(int mes){
        String mesS = new String();
        switch(mes){
            case 1: mesS = "Enero";
                    break;
            case 2: mesS = "Febrero";
                    break;
            case 3: mesS = "Marzo";
                    break;
            case 4: mesS = "Abril";
                    break;
            case 5: mesS = "Mayo";
                    break;
            case 6: mesS = "Junio";
                    break;
            case 7: mesS = "Julio";
                    break;
            case 8: mesS = "Agosto";
                    break;
            case 9: mesS = "Septiembre";
                    break;
            case 10: mesS = "Octubre";
                     break;
            case 11: mesS = "Noviembre";
                     break;
            case 12: mesS = "Diciembre";
                     break;
        }
        return mesS;
    }
    
    public void loadPagos(JTable tabla){
       DefaultTableModel modeloTabla = (DefaultTableModel)tabla.getModel();
        for (int i = modeloTabla.getRowCount() -1; i >=0; i--)
          modeloTabla.removeRow(i);
        PagoDAOImpl bdPay = new PagoDAOImpl();
        pagos = bdPay.getPagos(nino);
        Object[] columna = new Object[3];
        try{
            for(int i = 0; i<pagos.size(); i++){
                columna[0] = mesString(pagos.get(i).getMes());
                if (pagos.get(i).getForma_pago() == null){
                    columna[1] = calcularCosto(pagos.get(i).getMonto(), pagos.get(i).getMes());
                    columna[2] = "No";
                }
                else{
                    columna[1] = 0.00;
                    columna[2] = "Si";
                }
                modeloTabla.addRow(columna);
            }
        } catch(Exception e) {
            for (int i = modeloTabla.getRowCount() -1; i >=0; i--)
          modeloTabla.removeRow(i);

        }
    }
    
    public void pagarMensualidad(JTable tabla){
        LocalDate localDate = LocalDate.now();
        try{
            int index = tabla.getSelectedRow();
            Pago pay = pagos.get(index);
            pay.setFecha(Date.valueOf(localDate));
            pay.setForma_pago(String.valueOf(initialView.jComboTipoPago.getSelectedItem()));
            pay.setMonto(calcularCosto(pay.getMonto(), pay.getMes()));
            PagoDAOImpl dbPay = new PagoDAOImpl();
            dbPay.pagarMensualidad(pay);
            JOptionPane.showMessageDialog(initialView, "Datos cargads satisfactoriamente");
            loadPagos(tabla);
       } catch (Exception ex) {
           Logger.getLogger(MultaController.class.getName()).log(Level.SEVERE, null, ex);
           JOptionPane.showMessageDialog(initialView, "Error en los datos");
       }
        
    }
    
    public float calcularCosto(float costo, int mesMens){
        LocalDate localDate = LocalDate.now();
        int dia = localDate.getDayOfMonth();
        int mes = localDate.getMonthValue();
        if (mes > mesMens)
          costo = (float) (costo +costo*0.5);
        else if ((dia >= 10) && (dia<=20))
          costo = (float) (costo +costo*0.10);
        else if ((dia>=21) && (dia<=25))
          costo = (float) (costo +costo*0.2);
        else if (dia >=26)
          costo = (float) (costo +costo*0.3);
        return costo;
    }
    
}