package MainPackage;

import javax.swing.table.DefaultTableModel;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

public class TableExporter {
    public static void exportToCSV(DefaultTableModel model, File outFile) throws IOException {
        try (PrintWriter pw = new PrintWriter(outFile)) {
            for (int c = 0; c < model.getColumnCount(); c++) {
                pw.print(model.getColumnName(c));
                if (c < model.getColumnCount() - 1) pw.print(",");
            }
            pw.println();
            for (int r = 0; r < model.getRowCount(); r++) {
                for (int c = 0; c < model.getColumnCount(); c++) {
                    String cell = String.valueOf(model.getValueAt(r, c))
                            .replace("\"", "\"\"");
                    pw.print("\"" + cell + "\"");
                    if (c < model.getColumnCount() - 1) pw.print(",");
                }
                pw.println();
            }
        }
    }
}