package com.yunpeng.test.model.profile;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class PomProfileCheck {
    public static void main(String[] args) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = factory.newDocumentBuilder();
            Document doc = db.parse(new File("D:/project/invoice_branches/2016_1_6_dubbo","pom.xml"));
            Element elmtInfo = doc.getDocumentElement();
            NodeList n1s = elmtInfo.getChildNodes();
            int m = 1;
            for (int i = 0; i < n1s.getLength(); i++) {
                Node n1 = n1s.item(i);
                if (n1.getNodeName().equals("profiles")) {
                    NodeList n2s = n1.getChildNodes();
                    for (int j = 0; j < n2s.getLength(); j++) {
                        Node n2 = n2s.item(j);

                        if (n2.getNodeName().equals("profile")) {
                            NodeList n3s = n2.getChildNodes();
                            for (int k = 0; k < n3s.getLength(); k++) {
                                Node n3 = n3s.item(k);

                                if (n3.getNodeName().equals("id")) {
                                    System.out.print(m + "\t" + n3.getNodeName() + " = " + n3.getTextContent()+"\t" );
                                    m++;
                                }
                                if (n3.getNodeName().equals("properties")) {
                                    
                                    NodeList n4s = n3.getChildNodes();
                                    int commentSize=0;
                                    int propertiesSize=0;
                                    for (int l = 0; l < n4s.getLength(); l++) {
                                        Node n4 = n4s.item(l);

                                        if (n4.getNodeType() == Node.COMMENT_NODE) {
                                            commentSize++;
                                        }
                                        if (n4.getNodeType() == Node.ELEMENT_NODE) {
                                            propertiesSize++;
                                        }
                                    }
                                    
                                    System.out.print( "properties commentSize= "+ commentSize+", propertiesSize="+propertiesSize);
                                    System.out.println();
                                }
                            }
                        }
                    }
                }
            }
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}