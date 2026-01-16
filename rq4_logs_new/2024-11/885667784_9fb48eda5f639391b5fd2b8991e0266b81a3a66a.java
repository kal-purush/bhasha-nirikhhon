package Controladores;

import Vistas.VistaBalanzaComprobacion;
import daos.BalanzaComprobacionDAO;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseListener;
import java.util.ArrayList;
import javax.swing.table.DefaultTableModel;
import modelos.BalanzaComprobacion;

public class ControladorBalanzaC extends MouseAdapter implements ActionListener,
        MouseListener, KeyListener, ItemListener {

    VistaBalanzaComprobacion vista;
    DefaultTableModel modelo;
    BalanzaComprobacionDAO daoBalanza;
    ArrayList<BalanzaComprobacion> lista;

    public ControladorBalanzaC(VistaBalanzaComprobacion vista) {
        this.vista = vista;
        this.vista.btnMostrar.addActionListener(this);
        lista = new ArrayList();
        daoBalanza = new BalanzaComprobacionDAO();
    }

    public void mostrar() {
        this.vista.Tbalanza.setEnabled(false);

        double totalDebe = 0;
        double totalHaber = 0;
        double totalDeudor = 0;
        double totalAcreedor = 0;

        modelo = new DefaultTableModel();
        String titulo[] = {"CODIGO", "CUENTA", "DEBE", "HABER", "SALDO DEUDOR", "SALDO ACREEDOR"};
        modelo.setColumnIdentifiers(titulo);
        int i = 0;
        for (BalanzaComprobacion x : daoBalanza.obtenerBalanzaComprobacion()) {
            i++;
            Object datos[] = {x.getCodigo(), x.getCuenta(), x.getDebe(), x.getHaber(), x.getSaldoDeudor(), x.getSaldoAcreedor()};
            modelo.addRow(datos);
            totalDebe += x.getDebe();
            totalHaber += x.getHaber();
            totalDeudor += x.getSaldoDeudor();
            totalAcreedor += x.getSaldoAcreedor();
        }
        this.vista.Tbalanza.setModel(modelo);
        vista.tfDebe.setText(String.valueOf(totalDebe));
        vista.tfHaber.setText(String.valueOf(totalHaber));
        vista.tfDeudor.setText(String.valueOf(totalDeudor));
        vista.tfAcreedor.setText(String.valueOf(totalAcreedor));

        vista.tfDebe.setEnabled(false);
        vista.tfHaber.setEnabled(false);
        vista.tfDeudor.setEnabled(false);
        vista.tfAcreedor.setEnabled(false);

    }

    @Override
    public void actionPerformed(ActionEvent e) {
        if (e.getSource() == this.vista.btnMostrar) {
            mostrar();
        }
    }

    @Override
    public void keyTyped(KeyEvent e) {

    }

    @Override
    public void keyPressed(KeyEvent e) {

    }

    @Override
    public void keyReleased(KeyEvent e) {

    }

    @Override
    public void itemStateChanged(ItemEvent e) {

    }

}