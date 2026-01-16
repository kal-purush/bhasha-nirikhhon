package org.celllife.idart.gui.reportParameters;

import model.manager.reports.HistoricoLevantamentoXLS;
import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Row;
import org.celllife.idart.commonobjects.LocalObjects;
import org.celllife.idart.database.dao.ConexaoJDBC;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.widgets.Shell;
import org.vafada.swtcalendar.SWTCalendar;

import java.awt.*;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class HistoricoLevantamentoReferidosExcel implements IRunnableWithProgress {

    private List<HistoricoLevantamentoXLS> historicoLevantamentoReferidoXLS;
    private final Shell parent;
    private FileOutputStream out = null;
    private SWTCalendar swtCal;
    private String reportFileName;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private Date theStartDate;
    private Date theEndDate;

    SimpleDateFormat sdfYear = new SimpleDateFormat("yyyy");

    public HistoricoLevantamentoReferidosExcel(Shell parent, String reportFileName, Date theStartDate, Date theEndDate) {
        this.historicoLevantamentoReferidoXLS = historicoLevantamentoReferidoXLS;
        this.parent = parent;
        this.swtCal = swtCal;
        this.reportFileName = reportFileName;
        this.theStartDate = theStartDate;
        this.theEndDate = theEndDate;
    }

    @Override
    public void run(IProgressMonitor monitor) throws InvocationTargetException, InterruptedException {
        try {

            ConexaoJDBC con=new ConexaoJDBC();

            monitor.beginTask("Por Favor, aguarde ... ", 1);

            historicoLevantamentoReferidoXLS = con.getReferralHistoricoLevantamentosXLS(sdf.format(theStartDate), sdf.format(theEndDate));

            if(historicoLevantamentoReferidoXLS.size() > 0) {
                // Tell the user what you are doing
                monitor.beginTask("Carregando a lista... ", historicoLevantamentoReferidoXLS.size());

                FileInputStream currentXls = new FileInputStream(reportFileName);

                HSSFWorkbook workbook = new HSSFWorkbook(currentXls);

                HSSFSheet sheet = workbook.getSheetAt(0);

                HSSFCellStyle cellStyle = workbook.createCellStyle();
                cellStyle.setBorderBottom(BorderStyle.THIN);
                cellStyle.setBorderTop(BorderStyle.THIN);
                cellStyle.setBorderLeft(BorderStyle.THIN);
                cellStyle.setBorderRight(BorderStyle.THIN);
                cellStyle.setAlignment(HorizontalAlignment.CENTER);


                HSSFRow healthFacility = sheet.getRow(10);
                HSSFCell healthFacilityCell = healthFacility.createCell(2);
                healthFacilityCell.setCellValue(LocalObjects.currentClinic.getClinicName());
                healthFacilityCell.setCellStyle(cellStyle);

                HSSFRow reportPeriod = sheet.getRow(10);
                HSSFCell reportPeriodCell = reportPeriod.createCell(6);
                reportPeriodCell.setCellValue(sdf.format(theStartDate) + " à " + sdf.format(theEndDate));
                reportPeriodCell.setCellStyle(cellStyle);

                HSSFRow reportYear = sheet.getRow(11);
                HSSFCell reportYearCell = reportYear.createCell(6);
                reportYearCell.setCellValue(sdfYear.format(theStartDate));
                reportYearCell.setCellStyle(cellStyle);

                for (int i = 14; i <= sheet.getLastRowNum(); i++) {
                    HSSFRow row = sheet.getRow(i);
                    deleteRow(sheet, row);
                }

                out = new FileOutputStream(new File(reportFileName));
                workbook.write(out);

                int rowNum = 14;
                int i = 0;
                for (HistoricoLevantamentoXLS xls : historicoLevantamentoReferidoXLS) {
                    i++;
                    HSSFRow row = sheet.createRow(rowNum++);

                    HSSFCell createCellNid = row.createCell(1);
                    createCellNid.setCellValue(xls.getPatientIdentifier());
                    createCellNid.setCellStyle(cellStyle);

                    HSSFCell createCellNome = row.createCell(2);
                    createCellNome.setCellValue(xls.getNome() + " " + xls.getApelido());
                    createCellNome.setCellStyle(cellStyle);

                    HSSFCell createCellTipoTarv = row.createCell(3);
                    createCellTipoTarv.setCellValue(xls.getTipoTarv());
                    createCellTipoTarv.setCellStyle(cellStyle);

                    HSSFCell createCellRegimeTerapeutico = row.createCell(4);
                    createCellRegimeTerapeutico.setCellValue(xls.getRegimeTerapeutico());
                    createCellRegimeTerapeutico.setCellStyle(cellStyle);

                    HSSFCell createCellTipoDispensa = row.createCell(5);
                    createCellTipoDispensa.setCellValue(xls.getTipoDispensa());
                    createCellTipoDispensa.setCellStyle(cellStyle);

                    HSSFCell createCellDataLevantamento = row.createCell(6);
                    createCellDataLevantamento.setCellValue(xls.getDataLevantamento());
                    createCellDataLevantamento.setCellStyle(cellStyle);

                    HSSFCell createCellDataProximoLevantamento = row.createCell(7);
                    createCellDataProximoLevantamento.setCellValue(xls.getDataProximoLevantamento());
                    createCellDataProximoLevantamento.setCellStyle(cellStyle);

                    HSSFCell createCellReferencia = row.createCell(8);
                    createCellReferencia.setCellValue(xls.getClinic());
                    createCellReferencia.setCellStyle(cellStyle);

                    // Optionally add subtasks
                    monitor.subTask("Carregando : " + i + " de " + historicoLevantamentoReferidoXLS.size() + "...");

                    Thread.sleep(5);

                    // Tell the monitor that you successfully finished one item of "workload"-many
                    monitor.worked(1);
                    // Check if the user pressed "cancel"
                    if (monitor.isCanceled()) {
                        monitor.done();
                        return;
                    }
                }

                for (int i0 = 1; i0 < HistoricoLevantamentoXLS.class.getClass().getDeclaredFields().length; i0++) {
                    sheet.autoSizeColumn(i0);
                }

                monitor.done();
                currentXls.close();

                FileOutputStream outputStream = new FileOutputStream(new File(reportFileName));
                workbook.write(outputStream);
                workbook.close();

                Desktop.getDesktop().open(new File(reportFileName));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public List<HistoricoLevantamentoXLS> getList(){
        return this.historicoLevantamentoReferidoXLS;
    }

    private void deleteRow(HSSFSheet sheet, Row row) {
        int lastRowNum = sheet.getLastRowNum();
        if (lastRowNum > 0) {
            int rowIndex = row.getRowNum();
            HSSFRow removingRow = sheet.getRow(rowIndex);
            if (removingRow != null) {
                sheet.removeRow(removingRow);
            }
        }
    }

}