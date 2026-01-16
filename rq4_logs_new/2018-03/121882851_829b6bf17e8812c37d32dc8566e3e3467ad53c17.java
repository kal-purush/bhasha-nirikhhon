/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Controller;

import View.*;
import Model.*;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JComboBox;
import javax.swing.JOptionPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;

/**
 *
 * @author gabrielbaron
 */
public class AddMenuController {
    JDAddMenu vistaAm;
    ArrayList<String> rifs;
    GuarderiaDAOImpl modeloGuarderia = new GuarderiaDAOImpl();
    
    public AddMenuController(JDAddMenu vistaAm) {
        this.vistaAm = vistaAm;
        this.rifs = modeloGuarderia.getRifs();
    }
    
    public void llenarComboBoxGuarderias(JComboBox cb) {
        ArrayList<Guarderia> guarderias = new ArrayList();
        GuarderiaDAOImpl modeloGuarderia = new GuarderiaDAOImpl();
        guarderias = modeloGuarderia.loadGuarderias();
        int size = guarderias.size();
        cb.addItem("Guarderías");
        for (int i = 0; i < size; i++) {
            cb.addItem(guarderias.get(i).getComboText());
        }
    }
    
    public void mostrarplatos(JTable tabla){
        DefaultTableModel modeloTabla = (DefaultTableModel)tabla.getModel();
        for (int i = modeloTabla.getRowCount() -1; i >=0; i--)
          modeloTabla.removeRow(i);
        Object [] columna = new Object[2];
        try{
            PlatoDAOImpl bdPlato = new PlatoDAOImpl();
            ArrayList<Plato> platos = bdPlato.Platos_sin_menu();
            String Ingredientes = new String();
            for(int i = 0; i<platos.size(); i++){
                columna[0] = platos.get(i).getCodigo();
                for (int j=0;j<4;j++){
                    if (i!=3){ 
                     Ingredientes = Ingredientes + platos.get(i).getComidas().get(j).getNombre() + ",";
                    }          
                    else{
                      Ingredientes = Ingredientes + platos.get(i).getComidas().get(j).getNombre();
                    }
                    //System.out.println(platos.get(i).getCodigo());
                }
                columna[1] = Ingredientes;
                modeloTabla.addRow(columna);
            }
        } catch(Exception e){
            for (int i = modeloTabla.getRowCount() -1; i >=0; i--)
              modeloTabla.removeRow(i);
        }
    }
    
    public void platosClickTabla(JTable tabla){
        if (this.vistaAm.Plato1Txt.getText().length() == 0){
            this.vistaAm.Plato1Txt.setText(tabla.getValueAt(tabla.getSelectedRow(), 0).toString());
        }
        else
        {
           if (this.vistaAm.Plato2Txt.getText().length() == 0){
            this.vistaAm.Plato2Txt.setText(tabla.getValueAt(tabla.getSelectedRow(), 0).toString());
           }
           else
           {
                if (this.vistaAm.Plato3Txt.getText().length() == 0){
                  this.vistaAm.Plato3Txt.setText(tabla.getValueAt(tabla.getSelectedRow(), 0).toString());
                }
                else
                {
                    if (this.vistaAm.Plato4Txt.getText().length() == 0){
                      this.vistaAm.Plato4Txt.setText(tabla.getValueAt(tabla.getSelectedRow(), 0).toString());
                    }
                    else
                    {
                         if (this.vistaAm.Plato5Txt.getText().length() == 0){
                            this.vistaAm.Plato5Txt.setText(tabla.getValueAt(tabla.getSelectedRow(), 0).toString());
                         }
                    }
                }
           }
        }     
    }
    public void agregarMenu()
    {
        Menu menu = new Menu();
        ArrayList<Integer> platos = new ArrayList();
        try{
            menu.setCosto(Float.parseFloat(vistaAm.CostoTxt.getText()));
            menu.setFecha(vistaAm.Fecha_inicioTxt.getText());
            menu.setFecha_fin(vistaAm.Fecha_finTxt.getText());
            menu.setRif(rifs.get(vistaAm.guarderiaCombo.getSelectedIndex() - 1));
            platos.add(Integer.parseInt(vistaAm.Plato1Txt.getText()));
            platos.add(Integer.parseInt(vistaAm.Plato2Txt.getText()));
            platos.add(Integer.parseInt(vistaAm.Plato3Txt.getText()));
            platos.add(Integer.parseInt(vistaAm.Plato4Txt.getText()));
            platos.add(Integer.parseInt(vistaAm.Plato5Txt.getText()));
            MenuDAOImpl bdMenu = new MenuDAOImpl();
            if (bdMenu.VerificarFecha(menu.getFecha()).size()==0 && bdMenu.VerificarFecha(menu.getFecha_fin()).size()==0){
                bdMenu.InsertarMenu(menu,platos);
                JOptionPane.showMessageDialog(vistaAm, "Datos cargados satisfactoriamente");
                vistaAm.dispose();
            }
            else {
                 JOptionPane.showMessageDialog(vistaAm, "No Puede haber dos menus con fechas que coincidan");
            }       
        }catch(Exception e){
            Logger.getLogger(GuarderiaController.class.getName()).log(Level.SEVERE, null, e);
            JOptionPane.showMessageDialog(vistaAm, "Error en los datos");
        }   
        
    }
}