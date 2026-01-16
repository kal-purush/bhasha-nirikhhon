package com.example.myapplication.javatest;

import android.content.Context;
import android.content.res.XmlResourceParser;
import android.util.Log;
import android.util.Xml;

import com.example.myapplication.R;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;
import org.xmlpull.v1.XmlSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

/**
 * json和xml测试
 */
public class JsonAndXmlTest {

    public static final String TAG = JsonAndXmlTest.class.toString();

    /**
     * sax xml 解析
     * @param context
     */
    public void trySaxTest(Context context){
        //获取文件资源建立输入流对象
//        XmlResourceParser xml = context.getResources().getXml(R.xml.person);
        try (InputStream inputStream = context.getAssets().open("person.xml")) {
            //①创建XML解析处理器
            MySaxHandler mySaxHandler = new MySaxHandler();
            //②得到SAX解析工厂
            SAXParserFactory factory = SAXParserFactory.newInstance();
            //③创建SAX解析器
            SAXParser saxParser = factory.newSAXParser();
            //④将xml解析处理器分配给解析器,对文档进行解析,将事件发送给处理器
            saxParser.parse(inputStream,mySaxHandler);
            List<Person> list = mySaxHandler.getList();
            Log.d(TAG, "trySaxTest: last list is "+list.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        }
    }
    /**
     * sax解析xml：定义与注解再hdpi下的saxxml
     */
    public static class MySaxHandler extends DefaultHandler{
        private Person person;
        private List<Person> list;
        //解析当前的元素标签
        private String tags;

        public List<Person> getList() {
            return list;
        }

        /**
         * 当读取到文档开始标志是触发，通常在这里完成一些初始化操作
         */
        @Override
        public void startDocument() throws SAXException {
            super.startDocument();
            list = new ArrayList<>();
            Log.d(TAG, "startDocument: now start xml analysis");
        }
        /**
         * 读取到文档结尾时触发，
         */
        @Override
        public void endDocument() throws SAXException {
            super.endDocument();
            Log.d(TAG, "endDocument: now is endDocument ");
        }
        /**
         * 读到一个开始标签时调用,第二个参数为标签名,最后一个参数为属性数组
         */
        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            super.startElement(uri, localName, qName, attributes);
            //如果读到person标签
            if("person".equals(localName)){
                person = new Person();
                person.setId(Integer.parseInt(attributes.getValue("id")));
                Log.d(TAG, "startElement: now start do person");
            }
            //每个标签都改变，用于读取内容
            tags = localName;
        }
        /**
         * 处理元素结束时触发,这里将对象添加到结合中
         */
        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            super.endElement(uri, localName, qName);
            if("person".equals(localName)){
                list.add(person);
                person = null;
            }
            tags = null;
            Log.d(TAG, "endElement: now is endElement");
        }
        /**
         * 读到到内容,第一个参数为字符串内容,后面依次为起始位置与长度
         */
        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            super.characters(ch, start, length);
            //判断当前标签是否有效
            if(tags!=null){
                //将值转化为字符串
                String date = new String(ch,start,length);
                if("name".equals(tags)){
                    person.setName(date);
                    Log.d(TAG, "characters: now is do name");
                }else if("age".equals(tags)){
                    person.setAge(date);
                    Log.d(TAG, "characters: now is do age");
                }
            }
        }
    }

    public static class Person{
        private int id;
        private String name;
        private String age;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAge() {
            return age;
        }

        public void setAge(String age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", age='" + age + '\'' +
                    '}';
        }
    }

    /**
     * dom xml 解析：看hdpi下domxml
     * 从代码我们就可以看出DOM解析XML的流程，先整个文件读入Dom解析器，然后形成一棵树，
     * 然后我们可以遍历节点列表获取我们需要的数据!
     */
    public void tryDomXmlTest(Context context){
        List<Person> list = new ArrayList<>();
        //①获得DOM解析器的工厂示例:
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        try {
            //②从Dom工厂中获得dom解析器
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            //③把要解析的xml文件读入Dom解析器
            Document parse = documentBuilder.parse(context.getAssets().open("person.xml"));
            Log.d(TAG, "tryDomXmlTest: start do person");
            //④得到文档中名称为person的元素的结点列表
            NodeList person = parse.getElementsByTagName("person");
            //⑤遍历该集合,显示集合中的元素以及子元素的名字
            for (int i = 0; i<person.getLength();i++ ){
                Element item = (Element) person.item(i);
                //先从Person元素开始解析
                Person s = new Person();
                s.setId(Integer.parseInt(item.getAttribute("id")));
                //获取person下的name和age的Note集合
                NodeList childNodes = item.getChildNodes();
                for (int j = 0 ; j<childNodes.getLength();j++){
                    Node node = childNodes.item(j);
                    //判断子note类型是否为元素Note
                    if(node.getNodeType() == Node.ELEMENT_NODE){
                        Element e = (Element) node;
                        if("name".equals(e.getNodeName())){
                            s.setName(node.getNodeValue());
                        }else if("age".equals(node.getNodeName())){
                            s.setAge(node.getNodeValue());
                        }
                    }
                }
                list.add(s);
            }
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        }
    }

    /**
     * pull xml 解析 看hdpi下pullxml
     */
    public void tryPullXml(Context context){
        //获取解析器方法1
//        XmlPullParser xmlPullParser = Xml.newPullParser();
        List<Person> list = null;
        Person person = null;
        try {
            //获取解析器方法2
            XmlPullParserFactory factory = XmlPullParserFactory.newInstance();
            XmlPullParser xmlPullParser1 = factory.newPullParser();
            try (InputStream open = context.getAssets().open("person.xml")) {
                xmlPullParser1.setInput(open,"UTF-8");
                // 获得事件的类型
                int eventType = xmlPullParser1.getEventType();
                while (eventType!=XmlPullParser.END_DOCUMENT){
                    switch (eventType){
                        case XmlPullParser.START_DOCUMENT:
                            list = new ArrayList<>();
                            break;
                        case XmlPullParser.START_TAG:
                            if("person".equals(xmlPullParser1.getName())){
                                person = new Person();
                                //取出属性值
                                person.setId(Integer.parseInt(xmlPullParser1.getAttributeValue(0)));
                            }else if("name".equals(xmlPullParser1.getName())){
                                // 获取该节点的内容
                                String name = xmlPullParser1.nextText();
                                person.setName(name);
                            }else if("age".equals(xmlPullParser1.getName())){
                                person.setAge(xmlPullParser1.nextText());
                            }
                            break;
                        case XmlPullParser.END_DOCUMENT:
                            if("person".equals(xmlPullParser1.getName())){
                                list.add(person);
                                person = null;
                            }
                        default:
                            break;
                    }
                    xmlPullParser1.next();
                }
            }
        } catch (XmlPullParserException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * pull 生成 xml hdpi下pullshengchengxml
     */
    public void tryPullCreateXml(OutputStream outputStream,List<Person> list){
        XmlSerializer xmlSerializer = Xml.newSerializer();
        try {
            xmlSerializer.setOutput(outputStream,"UTF-8");
            xmlSerializer.startTag(null,"person");
            for (Person person : list){
                xmlSerializer.startTag(null,"person");
                xmlSerializer.attribute(null,"id",person.getId()+"");
                xmlSerializer.startTag(null,"name");
                xmlSerializer.text(person.getName());
                xmlSerializer.endTag(null,"name");
                xmlSerializer.startTag(null,"age");
                xmlSerializer.text(person.getAge());
                xmlSerializer.endTag(null,"age");
                xmlSerializer.endTag(null,"person");
            }
            xmlSerializer.endTag(null,"person");
            xmlSerializer.endDocument();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                outputStream.flush();
                outputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * XmlResourceParser:动态解析XML文件的工具
     * 常用的字段有：
     * int START_DOCUMENT = 0; //xml 文档开始标志
     * int END_DOCUMENT = 1; //xml 文档结束标志
     * int START_TAG = 2; //xml 标签开始标志
     * int END_TAG = 3; //xml 标签结束标志
     * int TEXT = 4;
     *常用方法：
     * int getEventType() //返回当前的事件类型，是上面的字段其中一个
     * String getName() //返回当前 TAG 的名字
     * String getText() //返回当前 text 的内容
     * String getAttributeName(int index) //返回指定位置的属性名，位置从0开始
     * String getAttributeValue(int index) //返回指定位置的属性值，位置从0开始
     * String getAttributeValue(String namespace,String name) //返回指定的属性名对应的属性值，如果没有使用命名空间，则第一个参数传入null
     * int next() //获取下一个要解析的事件，类似于光标下移
     */
    public void tryXmlResourceTest(Context context){
        XmlResourceParser xml = context.getResources().getXml(R.xml.person);
        List<Person> list = null;
        Person person = null;
        try {
            int eventType = xml.getEventType();
            while (eventType!=XmlResourceParser.END_DOCUMENT){
                switch (eventType){
                    case XmlResourceParser.START_DOCUMENT:
                        list = new ArrayList<>();
                        Log.d(TAG, "tryXmlResourceTest: now is start");
                        break;
                    case XmlResourceParser.START_TAG://一般都是获取标签的属性值，所以在这里数据你需要的数据
                        person = new Person();
                        Log.d(TAG, "tryXmlResourceTest: the " + xml.getAttributeName(0) + "is" + xml.getAttributeValue(0) );
                        person.setId(Integer.parseInt(xml.getAttributeValue(0)));
                        break;
                    case XmlResourceParser.TEXT:
                        if(xml.getName().equals("name")){
                            person.setName(xml.getText());
                        }else if("age".equals(xml.getName())){
                            person.setAge(xml.getText());
                        }
                        break;
                    case XmlResourceParser.END_TAG:
                        if(xml.getName().equals("person")){
                            list.add(person);
                            person = null;
                        }
                }
                eventType = xml.next();
            }
        } catch (XmlPullParserException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}