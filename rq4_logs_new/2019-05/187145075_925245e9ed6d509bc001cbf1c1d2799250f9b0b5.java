/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Data;

import com.sun.org.apache.bcel.internal.generic.AALOAD;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javafx.scene.image.Image;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.jdom.output.XMLOutputter;

/**
 *
 * @author fabian
 */
public class XMLCharacterManager {

    //variables
    private Document document;
    private Element root;
    private String path;

    public XMLCharacterManager(String path) throws IOException, JDOMException {
        //ruta en la que se encuentra el archivo XML
        this.path = path;

        File fileCharacter = new File(path);//esto es solo para hacer validacion
        if (fileCharacter.exists()) {
            //1. EL ARCHIVO YA EXISTE, ENTONCES LO CARGO EN MEMORIA

            //toma la estructura de datos y las carga en memoria
            SAXBuilder saxBuilder = new SAXBuilder();
            saxBuilder.setIgnoringElementContentWhitespace(true);

            //carga la memoria
            this.document = saxBuilder.build(path);
            this.root = this.document.getRootElement();
        } else {
            //2. EL ARCHIVO NO EXISTE, ENTONCES LO CREO Y LUEGO LO CARGO EN MEMORIA

            //CREAMOS EL ELEMENTO RAIZ
            this.root = new Element("Characters");

            //CREAMOS EL DOCUMENTO
            this.document = new Document(this.root);

            //GUARDAMOS EN DISCO DURO
            storeXML();
        }
    }//end constructor

    //almacena en disco duro nuestro documento xml en la ruta especificada
    private void storeXML() throws IOException {
        XMLOutputter xmlOutputter = new XMLOutputter();
        xmlOutputter.output(document, new PrintWriter(path));
    }

    //metodo para insertar un nuevo estudiante en el documento xml
    public void insertCharacter(Domain.Character character) throws IOException {
        //INSERTAMOS EN EL DOCUMENTO EN MEMORIA
        //para insertar en xml, primero se crean los elementos

        //crear el estudiante
        Element eCharacter = new Element("character");

        Element eIdentification = new Element("identification");
        eIdentification.addContent(character.getIdentification());

        //crear el elemento nombre
        Element eX = new Element("x");
        eX.addContent(String.valueOf(character.getX()));

        Element eY = new Element("y");
        eY.addContent(String.valueOf(character.getY()));

        Element eImgNum = new Element("imgNum");
        eImgNum.addContent(String.valueOf(character.getImgNum()));

        Element eSpeed = new Element("speed");
        eSpeed.addContent(String.valueOf(character.getSpeed()));

        //agregar al elemento student el contenido de nombre y nota
        eCharacter.addContent(eIdentification);
        eCharacter.addContent(eX);
        eCharacter.addContent(eY);
        eCharacter.addContent(eImgNum);
        eCharacter.addContent(eSpeed);

        //AGREGAMOS AL ROOT
        this.root.addContent(eCharacter);

        //FINALMENTE: GUARDAR EN DD
        storeXML();
    }//end insert

    //delete
    public void deleteStudent() throws IOException {
        List elementList = this.root.getChildren();
        elementList.remove(1);

        //FINALMENTE: GUARDAR EN DD
        storeXML();
    }

    //metodo para obtener todos los estudiantes en un arreglo
    public ArrayList<Domain.Character> getAllCharacters() {
        //obtenemos la cantidad de estudiantes

        //obtenemos una lista con todos los elementos de root
        List elementList = this.root.getChildren();

        //definimos el tamanno del arreglo
        ArrayList<Domain.Character> charactersArray = new ArrayList<>();

        //recorremos la lista para ir creando los objetos de tipo estudiante
        for (Object currentObject : elementList) {
            //transformo el object
            Element currentElement = (Element) currentObject;

            Domain.Character currentCharacter = new Domain.Character();

            currentCharacter.setIdentification(
                    currentElement.getAttributeValue("identification"));

            currentCharacter.setX(Integer.parseInt(currentElement.getChild("x").getValue()));

            currentCharacter.setY(Integer.parseInt(currentElement.getChild("y").getValue()));

            currentCharacter.setImgNum(Integer.parseInt(currentElement.getChild("imgNum").getValue()));

            currentCharacter.setSpeed(Integer.parseInt(currentElement.getChild("speed").getValue()));

            //guardar en el arreglo
            charactersArray.add(currentCharacter);
        }//end for
        return charactersArray;
    }
}