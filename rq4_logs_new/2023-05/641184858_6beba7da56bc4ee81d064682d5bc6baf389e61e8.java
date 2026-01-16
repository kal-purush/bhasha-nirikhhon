package ufes.adapter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.swing.JOptionPane;
import ufes.adapter.ILog;

/**
 *
 * @author Danilo-Js
 */
public class LogJSON implements ILog {

    private String path = Paths.get(System.getProperty("user.dir")).toString();    
    private String file = "/LogJSON.json";

    @Override
    public void escrever(String operation, String value) {
        // formata o log
        String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        String time = new SimpleDateFormat("HH:mm:ss").format(new Date());
        String logEntry = createLog(date, time, operation, value);
        
        // escreve o log
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path + file, true))) {
            writer.write(logEntry);
            writer.newLine();
        } catch (IOException e) {
            System.out.println("Erro ao salvar log: " + e);
            JOptionPane.showMessageDialog(null, "Erro ao salvar log: " + e);
        }
    }

    // pega a string a ser inserida
    @Override
    public String createLog(String date, String time, String operation, String value) {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
            .append("\"date\": \"").append(date).append("\",")
            .append("\"time\": \"").append(time).append("\",")
            .append("\"operation\": \"").append(operation).append("\",")
            .append("\"value\": \"").append(value).append("\"")
        .append("}");
        return builder.toString();
    }
}