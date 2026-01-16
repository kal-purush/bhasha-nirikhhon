package es.gameapp.multigameapp.extras;

import android.content.Context;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import es.gameapp.multigameapp.modelo.Palabra;
import es.gameapp.multigameapp.modelo.Pregunta;

public class InsertData {

    private DataManager db;
    private ArrayList<Pregunta> arrayPreguntas;
    private ArrayList<Palabra> arrayPalabras;


    public InsertData(Context context){
        db = new DataManager(context);
        arrayPreguntas = new ArrayList<>();
        arrayPalabras = new ArrayList<>();
    }

    public void cargarPreguntasSql(){
        //cargarArrayPreguntas();

        cargarArrayPreguntasXML();


        for (Pregunta pregunta: arrayPreguntas) {
            db.insertPregunta(pregunta);
        }
    }

    private void cargarArrayPreguntas(){
        String rutaArchivo = "raw/preguntas_trivia.txt";
        try {
            // Inicializamos file y reader
            FileReader file = null;
            file = new FileReader(rutaArchivo);
            BufferedReader reader = new BufferedReader(file);

            String line;
            try {
                while ((line = reader.readLine()) != null) {
                    String readedData[] = line.split(";");
                    Pregunta pregunta = new Pregunta();
                    pregunta.setTexto(readedData[1]);
                    pregunta.setOpcion1(readedData[2]);
                    pregunta.setOpcion2(readedData[3]);
                    pregunta.setOpcion3(readedData[4]);
                    pregunta.setOpcion4(readedData[5]);
                    pregunta.setRespuesta(readedData[6]);

                    arrayPreguntas.add(pregunta);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Cerramos file y reader
            file.close();
            reader.close();
        } catch (FileNotFoundException e) {
            System.out.println("File not found");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("File exception");
            e.printStackTrace();
        }
    }

    private void cargarArrayPreguntasXML() {
        String ruta ="raw/preguntas_trivia.xml";
        File archivo = new File(ruta);
        DocumentBuilderFactory dbFactoria = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = null;
        Document doc = null;
        try {
            dBuilder = dbFactoria.newDocumentBuilder();
            doc = dBuilder.parse(archivo);
        } catch (ParserConfigurationException | IOException | SAXException e) {
            e.printStackTrace();
        }

        doc.getDocumentElement().normalize();
        NodeList nList = doc.getElementsByTagName("pregunta");

        for (int temp = 0; temp < nList.getLength(); temp++) {
            Node nNode = nList.item(temp);
            Pregunta pregunta = new Pregunta();
            if (nNode.getNodeType() == Node.ELEMENT_NODE) {

                Element elementPregunta = (Element) nNode;

                pregunta.setTexto(elementPregunta.getElementsByTagName("texto").item(0).getTextContent());
                pregunta.setOpcion1(elementPregunta.getElementsByTagName("opcion1").item(0).getTextContent());
                pregunta.setOpcion2(elementPregunta.getElementsByTagName("opcion2").item(0).getTextContent());
                pregunta.setOpcion3(elementPregunta.getElementsByTagName("opcion3").item(0).getTextContent());
                pregunta.setOpcion4(elementPregunta.getElementsByTagName("opcion4").item(0).getTextContent());
                pregunta.setRespuesta(elementPregunta.getElementsByTagName("respuesta").item(0).getTextContent());
            }
            arrayPreguntas.add(pregunta);
        }
    }

}