package Controladores;

import Utilidades.SubCuentas;
import Vistas.VistaSubCuentas;
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

/**
 *
 * @author 22269
 */
public class ControladorSubCuentas extends MouseAdapter implements ActionListener,
        MouseListener, KeyListener, ItemListener {

    VistaSubCuentas vista;
    DefaultTableModel modelo;
    BalanzaComprobacionDAO daoBalanza;
    ArrayList<SubCuentas> lista;

    public ControladorSubCuentas(VistaSubCuentas vista) {
        this.vista = vista;
        this.vista.btnSubCuentas.addActionListener(this); // Escuchar el botón
        lista = new ArrayList<>();
        daoBalanza = new BalanzaComprobacionDAO(); // Inicializar DAO
    }

    public void mostrar() {
        // Habilitar la tabla
        this.vista.tbSubcuentas.setEnabled(true);

        // Configurar el modelo de la tabla
        modelo = new DefaultTableModel();
        String[] titulo = {"CODIGO", "CUENTA", "DEBE", "HABER"};
        modelo.setColumnIdentifiers(titulo);
        this.vista.tbSubcuentas.setModel(modelo);

        // Llenar la tabla con datos obtenidos del DAO
        lista = daoBalanza.obtenerSubCuentas(); // Obtener lista de subcuentas
        for (SubCuentas x : lista) {
            Object[] datos = {x.getCodigo(), x.getNombre(), x.getDebe(), x.getHaber()};
            modelo.addRow(datos); // Agregar filas al modelo
        }
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        if (e.getSource() == this.vista.btnSubCuentas) {
            mostrar(); // Llamar a mostrar al hacer clic en el botón
        }
    }

    @Override
    public void keyTyped(KeyEvent e) {
        // Método vacío, puedes implementarlo según necesidades
    }

    @Override
    public void keyPressed(KeyEvent e) {
        // Método vacío, puedes implementarlo según necesidades
    }

    @Override
    public void keyReleased(KeyEvent e) {
        // Método vacío, puedes implementarlo según necesidades
    }

    @Override
    public void itemStateChanged(ItemEvent e) {
        // Método vacío, puedes implementarlo según necesidades
    }
}