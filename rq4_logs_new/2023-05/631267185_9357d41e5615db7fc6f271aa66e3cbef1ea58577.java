/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package model;

/**
 *
 * @author aleja
 */
    import java.io.FileReader;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class InventarioPanel {
    public static void main(String[] args) {
        JSONParser parser = new JSONParser();

        try {
            // Leemos el archivo JSON
            Object obj = parser.parse(new FileReader("src/File/Inventario.txt"));
            JSONArray inventario = (JSONArray) obj;

            // Creamos una matriz para almacenar la información del inventario
            Object[][] data = new Object[inventario.size()][5];

            // Recorremos el inventario y almacenamos la información en la matriz
            for (int i = 0; i < inventario.size(); i++) {
                JSONObject producto = (JSONObject) inventario.get(i);
                data[i][0] = producto.get("RecibidoPor");
                data[i][1] = producto.get("contactoCliente");
                data[i][2] = producto.get("EntregadoPor");
                     data[i][2] = producto.get("EntregadoPor");
                          data[i][2] = producto.get("EntregadoPor");
            }

            // Creamos la tabla y le pasamos la matriz de datos
            String[] columnNames = {"Producto", "Cantidad", "Precio"};
            JTable table = new JTable(data, columnNames);

            // Creamos un JScrollPane para agregar la tabla
            JScrollPane scrollPane = new JScrollPane(table);

            // Creamos el marco y le agregamos el JScrollPane
            JFrame frame = new JFrame("Inventario");
            frame.add(scrollPane);

            // Configuramos el marco
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.pack();
            frame.setVisible(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

