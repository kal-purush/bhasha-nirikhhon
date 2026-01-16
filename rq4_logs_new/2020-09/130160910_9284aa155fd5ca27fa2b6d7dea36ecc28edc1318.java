package fi.metatavu.metaform.server.xlsx;

import java.io.IOException;
import java.io.OutputStream;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder for Excel spreadsheets
 * 
 * @author Antti Leppä
 */
public abstract class AbstractXlsxBuilder<B extends org.apache.poi.ss.usermodel.Workbook, S extends org.apache.poi.ss.usermodel.Sheet> implements AutoCloseable {

  private static Logger logger = LoggerFactory.getLogger(AbstractXlsxBuilder.class);

  private B workbook;
  private Map<String, S> sheets;
  private Map<String, Row> rows;
  private Map<String, Cell> cells;
  private CellStyle dateTimeCellStyle;
  
  /**
   * Constructor
   * 
   * @param workbook instance
   */
  public AbstractXlsxBuilder(B workbook) {
    this.workbook = workbook;
    this.sheets = new HashMap<>();
    this.rows = new HashMap<>();
    this.cells = new HashMap<>();

    CreationHelper createHelper = workbook.getCreationHelper();

    this.dateTimeCellStyle = workbook.createCellStyle();
    this.dateTimeCellStyle.setDataFormat(createHelper.createDataFormat().getFormat("DD.MM.YYYY HH:MM"));
  }

  /**
   * Creates new sheet
   * 
   * @param id sheet id
   * @param label sheet label
   */
  @SuppressWarnings("unchecked")
  public String createSheet(String label) {
    String id = UUID.randomUUID().toString();
    sheets.put(id, (S) workbook.createSheet(label));
    return id;
  }

  /**
   * Sets a cell value
   * 
   * @param sheetId sheet id
   * @param rowNumber row number
   * @param columnNumber column number
   * @param value value
   * @return cell
   */
  public Cell setCellValue(String sheetId, int rowNumber, int columnNumber, String value) {
    Cell cell = findOrCreateCell(sheetId, rowNumber, columnNumber);
    if (cell != null) {
      cell.setCellValue(value);
    }
    
    return cell;
  }

  /**
   * Sets a cell value
   * 
   * @param sheetId sheet id
   * @param rowNumber row number
   * @param columnNumber column number
   * @param value value
   * @return cell
   */
  public Cell setCellValue(String sheetId, int rowNumber, int columnNumber, OffsetDateTime value) {
    return setCellValue(sheetId, rowNumber, columnNumber, Date.from(value.toInstant()));
  }
  
  /**
   * Sets a cell value
   * 
   * @param sheetId sheet id
   * @param rowNumber row number
   * @param columnNumber column number
   * @param value value
   * @return cell
   */
  public Cell setCellValue(String sheetId, int rowNumber, int columnNumber, Date value) {
    Cell cell = findOrCreateCell(sheetId, rowNumber, columnNumber);
    if (cell != null) {
      cell.setCellValue(value);
      cell.setCellStyle(this.dateTimeCellStyle);
    }

    return cell;
  }

  /**
   * Sets a cell value
   * 
   * @param sheetId sheet id
   * @param rowNumber row number
   * @param columnNumber column number
   * @param value value
   * @return cell
   */
  public Cell setCellValue(String sheetId, int rowNumber, int columnNumber, Boolean value) {
    Cell cell = findOrCreateCell(sheetId, rowNumber, columnNumber);
    if (cell != null) {
      cell.setCellValue(value);
    }
    
    return cell;
  }

  /**
   * Sets a cell value
   * 
   * @param sheetId sheet id
   * @param rowNumber row number
   * @param columnNumber column number
   * @param value value
   * @return cell
   */
  public Cell setCellValue(String sheetId, int rowNumber, int columnNumber, Double value) {
    Cell cell = findOrCreateCell(sheetId, rowNumber, columnNumber);
    if (cell != null) {
      cell.setCellValue(value);
    }

    return cell;
  }

  /**
   * Sets a cell value
   * 
   * @param sheetId sheet id
   * @param rowNumber row number
   * @param columnNumber column number
   * @param value value
   * @return cell
   */
  public Cell setCellValue(String sheetId, int rowNumber, int columnNumber, Integer value) {
    Cell cell = findOrCreateCell(sheetId, rowNumber, columnNumber);
    if (cell != null) {
      cell.setCellValue(value);
    }

    return cell;
  }

  /**
   * Sets a cell value
   * 
   * @param sheetId sheet id
   * @param rowNumber row number
   * @param columnNumber column number
   * @param value value
   * @return cell
   */
  public Cell setCellValue(String sheetId, int rowNumber, int columnNumber, Long value) {
    Cell cell = findOrCreateCell(sheetId, rowNumber, columnNumber);
    if (cell != null) {
      cell.setCellValue(value);
    }

    return cell;
  }

  /**
   * Writes sheet into stream
   * 
   * @param stream stream
   * @throws IOException thrown when writing fails
   */
  public void write(OutputStream stream) throws IOException {
    workbook.write(stream);
  }

  @Override
  public void close() throws Exception {
    workbook.close();
  }

  /**
   * Finds or creates cell by sheet id, row number and column number
   * 
   * @param sheetId sheet id
   * @param rowNumber row number
   * @param columnNumber column number
   * @return cell
   */
  private Cell findOrCreateCell(String sheetId, int rowNumber, int columnNumber) {
    Row row = findOrCreateRow(sheetId, rowNumber);
    if (row == null) {
      return null;
    }

    String key = String.format("%s-%d-%d", sheetId, rowNumber, columnNumber);

    if (!cells.containsKey(key)) {
      Cell cell = row.createCell(columnNumber);
      cells.put(key, cell);
    }

    return cells.get(key);
  }

  /**
   * Finds or creates row sheet id and row number
   * 
   * @param sheetId sheet id
   * @param rowNumber row number
   * @return row
   */
  private Row findOrCreateRow(String sheetId, int rowNumber) {
    S sheet = getSheet(sheetId);
    if (sheet == null) {
      logger.error("Could not find sheet {}", sheetId);
      return null;
    }

    String key = String.format("%s-%s", sheetId, rowNumber);

    if (!rows.containsKey(key)) {
      rows.put(key, sheet.createRow(rowNumber));
    }

    return rows.get(key);
  }

  /**
   * Returns sheet by sheetId
   * 
   * @param sheetId sheet id
   * @return sheet or null if not found
   */
  private S getSheet(String sheetId) {
    return sheets.get(sheetId);
  }

}