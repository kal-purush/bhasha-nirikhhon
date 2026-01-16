package Controladores;

import Vistas.VistaLibroDiario;
import daos.DaoLibroDiario;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import javax.swing.JOptionPane;
import javax.swing.table.DefaultTableModel;
import modelos.LibroDiario;

public class ControladorLibroDiario implements ActionListener {

    VistaLibroDiario frmLibro;
    DaoLibroDiario daoLibro;
    private ArrayList<LibroDiario> listaLibroDiario = new ArrayList<>();
    private String conceptoGlobal = "";

    public ControladorLibroDiario(VistaLibroDiario vista) {
        this.frmLibro = vista;
        this.frmLibro.btnAgregar.addActionListener(this);
        this.frmLibro.btnProcesarPartida.addActionListener(this);
        daoLibro = new DaoLibroDiario();
        mostrarNumeroPartida();
        mostrarPartidaAnterior();
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        if (e.getSource() == frmLibro.btnAgregar) {
            agregarDatosTabla();
        } else if (e.getSource() == frmLibro.btnProcesarPartida) {
            procesarPartida();
        }
    }

    public void agregarDatosTabla() {
        try {
            Date fechaSeleccionada = frmLibro.tfFecha.getDate();
            int codigoSubcuenta = Integer.parseInt(frmLibro.cbCodigo.getSelectedItem().toString());
            String nombreCuenta = frmLibro.tfCuenta.getText();
            double monto = Double.parseDouble(frmLibro.tfMonto.getText());
            String transaccion = frmLibro.cbTransaccion.getSelectedItem().toString();
            if (!frmLibro.tfConcepto.getText().isEmpty()) {
                conceptoGlobal = frmLibro.tfConcepto.getText();
                frmLibro.tfConcepto.setEnabled(false);
            }

            String concepto = conceptoGlobal;

            SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
            String fechaFormateada = formatoFecha.format(fechaSeleccionada);

            Object[] fila = new Object[7];
            fila[0] = fechaFormateada;
            fila[1] = codigoSubcuenta;
            fila[2] = nombreCuenta;
            fila[5] = concepto;

            double montoDebe = 0;
            double montoHaber = 0;

            if ("Debe".equals(transaccion)) {
                montoDebe = monto;
                fila[3] = monto;
                fila[4] = "";
            } else if ("Haber".equals(transaccion)) {
                montoHaber = monto;
                fila[3] = "";
                fila[4] = monto;
            }

            DefaultTableModel modeloTabla = (DefaultTableModel) frmLibro.tbDatos.getModel();
            modeloTabla.addRow(fila);

            listaLibroDiario.add(new LibroDiario(
                    Integer.parseInt(frmLibro.tfNumeroPartida.getText()),
                    fechaSeleccionada,
                    codigoSubcuenta,
                    nombreCuenta,
                    monto,
                    transaccion,
                    concepto
            ));

            actualizarSumas();
            verificarArrayList();

            frmLibro.cbCodigo.setSelectedIndex(0);
            frmLibro.tfMonto.setText("");
            frmLibro.cbTransaccion.setSelectedIndex(0);
            frmLibro.tfCuenta.setText("");

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Error al capturar o agregar datos: " + ex.getMessage());
        }
    }

    public void procesarPartida() {
        try {
            //Para ver si el debe y haber son iguales
            double totalDebe = 0.0;
            double totalHaber = 0.0;

            for (int i = 0; i < frmLibro.tbDatos.getRowCount(); i++) {
                String debe = frmLibro.tbDatos.getValueAt(i, 3).toString();
                String haber = frmLibro.tbDatos.getValueAt(i, 4).toString();
                if (!debe.isEmpty()) {
                    totalDebe += Double.parseDouble(debe);
                }
                if (!haber.isEmpty()) {
                    totalHaber += Double.parseDouble(haber);
                }
            }
            if (totalDebe != totalHaber) {
                JOptionPane.showMessageDialog(null, "El total del debe y el haber no son iguales. No se puede procesar.");
                return;
            }
            int numeroPartida = daoLibro.obtenerUltimaPartida();
            numeroPartida++;
            frmLibro.tfNumeroPartida.setText(String.valueOf(numeroPartida));
            for (LibroDiario libro : listaLibroDiario) {
                libro.setNumeroPartida(numeroPartida);

                String resultado = daoLibro.insertarLibroDiario(libro);
                if ("exito".equals(resultado)) {
                    System.out.println("Partida guardada correctamente: " + libro);
                } else {
                    System.out.println("Error al guardar la partida: " + libro);
                }
            }
            JOptionPane.showMessageDialog(null, "Partida procesada correctamente.");
            int respuesta = JOptionPane.showConfirmDialog(null, "¿Desea agregar otra partida?", "Agregar Partida", JOptionPane.YES_NO_OPTION);
            if (respuesta == JOptionPane.YES_OPTION) {
                limpiarCampos();
                frmLibro.tfPartidaAnterior.setText(String.valueOf(numeroPartida));
                int nuevoNumeroPartida = numeroPartida + 1;
                frmLibro.tfNumeroPartida.setText(String.valueOf(nuevoNumeroPartida));
                frmLibro.tfConcepto.setEnabled(true);
            } else {
                limpiarCampos();
                frmLibro.tfPartidaAnterior.setText(String.valueOf(numeroPartida));
                int nuevoNumeroPartida = numeroPartida + 1;
                frmLibro.tfNumeroPartida.setText(String.valueOf(nuevoNumeroPartida));
                frmLibro.tfConcepto.setEnabled(true);
            }

        } catch (Exception e) {
            e.printStackTrace();
            JOptionPane.showMessageDialog(null, "Error al procesar las partidas: " + e.getMessage());
        }
    }

    public void limpiarCampos() {
        frmLibro.cbCodigo.setSelectedIndex(0);
        frmLibro.tfCuenta.setText("");
        frmLibro.tfMonto.setText("");
        frmLibro.cbTransaccion.setSelectedIndex(0);
        frmLibro.tfConcepto.setText("");
        frmLibro.tfSumaDebe.setText("");
        frmLibro.tfSumaHaber.setText("");
        DefaultTableModel modeloTabla = (DefaultTableModel) frmLibro.tbDatos.getModel();
        modeloTabla.setRowCount(0);
    }

    //Solo para ver si me guarda en el ArrayList
    public void verificarArrayList() {
        for (LibroDiario transaccion : listaLibroDiario) {
            System.out.println("Número de Partida: " + transaccion.getNumeroPartida());
            System.out.println("Fecha: " + transaccion.getFecha());
            System.out.println("Código Subcuenta: " + transaccion.getCodSubcuenta());
            System.out.println("Nombre Cuenta: " + transaccion.getNombreCuenta());
            System.out.println("Monto: " + transaccion.getMonto());
            System.out.println("Transacción: " + transaccion.getTransaccion());
            System.out.println("Concepto: " + transaccion.getConcepto());
            System.out.println("------------------------------------------------");
        }
    }

    public void actualizarSumas() {
        double sumaDebe = 0;
        double sumaHaber = 0;
        DefaultTableModel modeloTabla = (DefaultTableModel) frmLibro.tbDatos.getModel();
        for (int i = 0; i < modeloTabla.getRowCount(); i++) {
            String debe = modeloTabla.getValueAt(i, 3).toString();
            String haber = modeloTabla.getValueAt(i, 4).toString();

            if (!debe.isEmpty()) {
                sumaDebe += Double.parseDouble(debe);
            }
            if (!haber.isEmpty()) {
                sumaHaber += Double.parseDouble(haber);
            }
        }
        frmLibro.tfSumaDebe.setText(String.format("%.2f", sumaDebe));
        frmLibro.tfSumaHaber.setText(String.format("%.2f", sumaHaber));
    }

    public void mostrarNumeroPartida() {
        try {
            int ultimaPartida = daoLibro.obtenerUltimaPartida();
            int nuevoNumeroPartida = ultimaPartida + 1;
            frmLibro.tfNumeroPartida.setText(String.valueOf(nuevoNumeroPartida));
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Error al obtener el número de partida: " + ex.getMessage());
        }
    }

    public void mostrarPartidaAnterior() {
        try {
            int ultimaPartida = daoLibro.obtenerUltimaPartida();
            frmLibro.tfPartidaAnterior.setText(String.valueOf(ultimaPartida));
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Error al obtener el último número de partida: " + ex.getMessage());
        }
    }

}