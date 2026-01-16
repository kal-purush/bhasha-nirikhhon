package Server;

import CentralPoint.*;
import java.io.StringWriter;
import java.util.ArrayList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ListPeerManager {
    private ArrayList<PeerInfo> lstPeer;
    private String datapath;
    private UserDatabase database;
    
    public ListPeerManager(String datapath) {
        this.datapath = datapath;
        database = new UserDatabase(datapath);
        lstPeer = new ArrayList<>();
    }
    
    public boolean login(PeerInfo user) {
        if (database.checkLogin(user.getUsername(), user.getPassword())) {
            lstPeer.add(user);
            return true;
        }
        else {
            return false;
        }
    }
    
    public boolean register(PeerInfo user) {
        if (database.addUser(user.getUsername(), user.getPassword())) {
            lstPeer.add(user);
            return true;
        }
        else {
            return false;
        }
    }
    
    public boolean logout(PeerInfo user) {
        return lstPeer.remove(user);
    }
    
    public String createPeerListXML() throws Exception {
        String res = "";
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.newDocument();
        Element rootElement = doc.createElement(ConstantTags.ONLINE_PEER_TAG);
        doc.appendChild(rootElement);
        
        for (PeerInfo peer : lstPeer) {
            Element newElem = doc.createElement(ConstantTags.PEER_TAG);
            newElem.appendChild(createNode(ConstantTags.USERNAME_TAG, peer.getUsername(), doc));
            newElem.appendChild(createNode(ConstantTags.IP_TAG, peer.getIP(), doc));
            newElem.appendChild(createNode(ConstantTags.PORT_TAG, String.valueOf(peer.getPortNum()), doc));
            
            rootElement.appendChild(newElem);
        }
        
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(doc);
            
            StringWriter outWriter = new StringWriter();
            StreamResult result = new StreamResult(outWriter);
            
            transformer.transform(source, result);  
            
            StringBuffer sb = outWriter.getBuffer(); 
            
            return sb.toString();
    }
    
    private Element createNode(String tag, String content, Document doc) throws ParserConfigurationException {       
        Element elem = doc.createElement(tag);
        elem.appendChild(doc.createTextNode(content));
        
        return elem;
    }
}