package com.company;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;

public class XMLFileReader {

    public static void main(String[] args) throws ParserConfigurationException, IOException, SAXException {
        File xmlFile = new File("C:\\Users\\Sushobhan\\IdeaProjects\\XMLFileHandlingJava\\src\\com\\company\\Output\\data.xml");
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
        Document document = documentBuilder.parse(xmlFile);

        NodeList list = document.getElementsByTagName("Developers");

        for(int i=0;i<list.getLength();i++){
            Node node = list.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE){
                Element element = (Element) node;

                System.out.println("The Contents of the XML file are...");
                System.out.println("ID: " + element.getAttribute("Id"));
                System.out.println("Name: " + element.getElementsByTagName("Name").item(0).getTextContent());
                System.out.println("Surname: " + element.getElementsByTagName("Surname").item(0).getTextContent());
                System.out.println("Age: " + element.getElementsByTagName("Age").item(0).getTextContent());
            }
        }
    }
}