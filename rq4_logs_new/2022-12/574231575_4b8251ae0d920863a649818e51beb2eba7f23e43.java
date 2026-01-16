package modeloDAO;

import config.conexion;
import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import jxl.Workbook;
import jxl.write.WritableSheet;
import jxl.write.Number;
import jxl.write.Label;
import jxl.write.WritableWorkbook;
public class modeloMySqlAExcel {

    private static final File file = new File("C:\\");

    public static void Exportar() {
        int row = 0;
        WritableSheet excelSheet = null;
        WritableWorkbook workbook = null;
        try {
            workbook = Workbook.createWorkbook(file);
            workbook.createSheet("dato", 0);
            excelSheet = workbook.getSheet(0);
        } catch (Exception e) {
            System.err.println("Error: " + e);
        }
        String sql = "select * from dispositivo";
        try {
            PreparedStatement ps = conexion.Conectar().prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Number idDispositivo = new Number(0, row, rs.getLong("idDispositivo"));
                Label nombre = new Label(1, row, rs.getString("nombre"));
                Label marca = new Label(2, row, rs.getString("marca"));
                row++;
                try {
                    excelSheet.addCell(idDispositivo);
                    excelSheet.addCell(nombre);
                    excelSheet.addCell(marca);

                } catch (Exception e) {
                    System.err.println("Error: " + e);
                }
            }
            rs.close();
        } catch (Exception e) {
            System.err.println("Error: " + e);
        }
        try {
            workbook.write();
            workbook.close();
        } catch (Exception e) {
            System.err.println("Error: " + e);
        }
    }
}