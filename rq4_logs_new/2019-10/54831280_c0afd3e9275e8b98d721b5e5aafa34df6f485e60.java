package com.haothink.utils;

import de.odysseus.staxon.json.JsonXMLConfig;
import de.odysseus.staxon.json.JsonXMLConfigBuilder;
import de.odysseus.staxon.json.JsonXMLInputFactory;
import de.odysseus.staxon.json.JsonXMLOutputFactory;
import de.odysseus.staxon.xml.util.PrettyXMLEventWriter;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wanghao on 2019-10-22
 * mail:wanghaotime@qq.com
 **/
public class JsonInterchXmlUtil {

    public static void main(String[] args) {

        String xml="<ROOT>"+
                "<Status>00</Status>"+
                "<ErrorMsg></ErrorMsg>"+
                "<Data>"+
                "<Row>"+
                "<PERSONID>35020500200610000000000701355116</PERSONID>"+
                "<XM>吴聪楠</XM><SFZH>350624198908052530</SFZH>"+
                "</Row>"+
                "<Row>"+
                "<PERSONID>35020500200610000000000701355117</PERSONID>"+
                "<XM>吴聪楠2</XM><SFZH>350624198908052531</SFZH>"+
                "</Row>"+
                "</Data>"+
                "</ROOT>";


        System.out.println(xml2json(xml));
    }


    public static String json2xml(String json) {
        StringReader input = new StringReader(json);
        StringWriter output = new StringWriter();
        JsonXMLConfig config = new JsonXMLConfigBuilder().multiplePI(false).repairingNamespaces(false).build();
        try {
            XMLEventReader reader = new JsonXMLInputFactory(config).createXMLEventReader(input);
            XMLEventWriter writer = XMLOutputFactory.newInstance().createXMLEventWriter(output);
            writer = new PrettyXMLEventWriter(writer);
            writer.add(reader);
            reader.close();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                output.close();
                input.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return output.toString();
    }

    public static String json2xmlPay(String json) {
        String xmlStr = json2xml(json);
        if (xmlStr.length() >= 38) {// remove <?xml version="1.0" encoding="UTF-8"?>
            return "<xml>" + xmlStr.substring(39) + "</xml>";
        }
        return xmlStr;
    }

    public static String xml2json(String xml) {
        StringReader input = new StringReader(xml);
        StringWriter output = new StringWriter();
        JsonXMLConfig config = new JsonXMLConfigBuilder().autoArray(true).autoPrimitive(true).prettyPrint(true).build();
        try {
            XMLEventReader reader = XMLInputFactory.newInstance().createXMLEventReader(input);
            XMLEventWriter writer = new JsonXMLOutputFactory(config).createXMLEventWriter(output);
            writer.add(reader);
            reader.close();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                output.close();
                input.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return output.toString();
    }


    public static String json2xmlReplaceBlank(String json) {
        String str = json2xml(json);
        String dest = "";
        if (str != null) {
            Pattern p = Pattern.compile("\\s*|\t|\r|\n");
            Matcher m = p.matcher(str);
            dest = m.replaceAll("");
        }
        return dest;
    }

}