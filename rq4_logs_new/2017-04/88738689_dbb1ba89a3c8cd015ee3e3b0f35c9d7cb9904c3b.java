import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.util.List;

/**
 * Created by Administrator on 2017-04-19.
 */
public class Demoxml {
    public static void main(String[] args) throws DocumentException {
        //创建SAXReader对象
        SAXReader reader = new SAXReader();
        //通过read方法读取一个文件 转换成Document对象
       Document document =  reader.read(new File("src/main/resources/books.xml"));
       //获取根节点对象
        Element node = document.getRootElement();
        //根节点名称
//        System.out.println(node.getName());
       /* Element child = node.element("book");
        Element child1 = child.element("name");
        System.out.println(child1.getText());*/
        //子节点
        List childenNode =  node.elements();
        //System.out.println(Arrays.asList(childenNode));
        for (int i = 0; i <childenNode.size() ; i++) {
            Node childNode = (Node)childenNode.get(i);
            System.out.println(childNode.getName());
            Element childEle=(Element) childenNode.get(i);
            List childEleNode = childEle.elements();
            for (int j = 0; j <childEleNode.size() ; j++) {
                Node childNode1 = (Node)childEleNode.get(j);
                System.out.println(childNode1.getName()+"--------"+childNode1.getText());
            }

        }

    }

}