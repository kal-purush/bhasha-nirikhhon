package Controladores;

import Vistas.VistaMostrarLibroDiario;
import daos.DaoLibroDiario;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseListener;
import java.text.SimpleDateFormat;
import modelos.LibroDiario;
import javax.swing.table.DefaultTableModel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author kevin
 */
public class ControladorMostrarLibroDiario extends MouseAdapter implements ActionListener,
        MouseListener {

    VistaMostrarLibroDiario frmLibro;
    DaoLibroDiario daoLibro;
    LibroDiario libro;

    public ControladorMostrarLibroDiario(VistaMostrarLibroDiario frmLibro) {
        this.frmLibro = frmLibro;
        this.frmLibro.tbDatos.addMouseListener(this);
        daoLibro = new DaoLibroDiario();
        cargarTablaLibroDiario();
        actualizarSumas();
    }

    public void cargarTablaLibroDiario() {
        ArrayList<LibroDiario> listaPartidas = daoLibro.obtenerTodasLasPartidas();
        DefaultTableModel model = new DefaultTableModel();

        model.addColumn("Partida");
        model.addColumn("Fecha");
        model.addColumn("Código");
        model.addColumn("Cuentas");
        model.addColumn("Debe");
        model.addColumn("Haber");

        Map<Integer, ArrayList<LibroDiario>> partidasAgrupadas = new HashMap<>();
        for (LibroDiario libroDiario : listaPartidas) {
            partidasAgrupadas
                    .computeIfAbsent(libroDiario.getNumeroPartida(), k -> new ArrayList<>())
                    .add(libroDiario);
        }
        for (Map.Entry<Integer, ArrayList<LibroDiario>> entry : partidasAgrupadas.entrySet()) {
            Integer numeroPartida = entry.getKey();
            ArrayList<LibroDiario> transacciones = entry.getValue();

            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
            String fecha = sdf.format(transacciones.get(0).getFecha());
            String concepto = transacciones.get(0).getConcepto();

            Object[] partidaRow = new Object[6];
            partidaRow[0] = "" + numeroPartida;
            partidaRow[1] = fecha;
            partidaRow[2] = "";
            partidaRow[3] = "";
            partidaRow[4] = "";
            partidaRow[5] = "";
            model.addRow(partidaRow);
            for (int i = 0; i < transacciones.size(); i++) {
                LibroDiario libroDiario = transacciones.get(i);

                Object[] row = new Object[6];

                if (i == 0) {
                    row[0] = "";
                    row[1] = "";
                } else {
                    row[0] = "";
                    row[1] = "";
                }

                double monto = libroDiario.getMonto();

                row[2] = libroDiario.getCodSubcuenta();
                row[3] = libroDiario.getNombreCuenta();

                if ("DEBE".equalsIgnoreCase(libroDiario.getTransaccion())) {
                    row[4] = monto;
                    row[5] = null;
                } else if ("HABER".equalsIgnoreCase(libroDiario.getTransaccion())) {
                    row[4] = null;
                    row[5] = monto;
                }
                model.addRow(row);
            }
            Object[] conceptoRow = new Object[6];
            conceptoRow[0] = "Concepto: " + concepto;
            conceptoRow[1] = "";
            conceptoRow[2] = "";
            conceptoRow[3] = "";
            conceptoRow[4] = "";
            conceptoRow[5] = "";
            model.addRow(conceptoRow);
        }
        frmLibro.tbDatos.setModel(model);
    }

    public void actualizarSumas() {
        double sumaDebe = 0;
        double sumaHaber = 0;
        DefaultTableModel modeloTabla = (DefaultTableModel) frmLibro.tbDatos.getModel();
        for (int i = 0; i < modeloTabla.getRowCount(); i++) {
            Object debeObj = modeloTabla.getValueAt(i, 4);
            Object haberObj = modeloTabla.getValueAt(i, 5);
            if (debeObj != null && !debeObj.toString().isEmpty()) {
                sumaDebe += Double.parseDouble(debeObj.toString());
            }
            if (haberObj != null && !haberObj.toString().isEmpty()) {
                sumaHaber += Double.parseDouble(haberObj.toString());
            }
        }
        frmLibro.tfSumaDebe.setText(String.format("%.2f", sumaDebe));
        frmLibro.tfSumaHaber.setText(String.format("%.2f", sumaHaber));
    }

    @Override
    public void actionPerformed(ActionEvent e) {

    }

    @Override
    public void mouseClicked(java.awt.event.MouseEvent e) {

    }
}