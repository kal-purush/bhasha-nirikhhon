package jp.hashiwa.nn.graph;

import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.IOException;

/**
 * Created by Hashiwa on 2015/09/01.
 */
public class GraphIO {
  private static final String GRAPH_TAG = "graph";
  private static final String NODE_TAG = "node";
  private static final String ID_TAG = "id";
  private static final String CLASS_TAG = "class";
  private static final String WEIGHT_TAG = "weight";

  public static GraphWriter getWriter(String filename) {
    try {
      return new GraphIO(filename).new GraphWriter();
    } catch (ParserConfigurationException e) {
      return null;
    }
  }

  public static GraphReader getReader(String filename) {
    try {
      return new GraphIO(filename).new GraphReader();
    } catch (ParserConfigurationException e) {
      return null;
    }
  }

  private final File file;
  private final DocumentBuilder documentBuilder;

  private GraphIO(String filename) throws ParserConfigurationException {
    this.file = new File(filename);
    this.documentBuilder = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder();
  }

  private class GraphWriter {
    private GraphWriter() {}

    public void write(Graph g) throws IOException {
      Document document = documentBuilder.newDocument();
      TransformerFactory transformerFactory = TransformerFactory
              .newInstance();

      Element graphElem = document.createElement(GRAPH_TAG);
      document.appendChild(graphElem);

      for (int i=0 ; i<g.getOutputNodeNum() ; i++) {
        NNLayerNode n = g.getOutputNode(i);
        Element nodeElem = createNodeElement(document, n);
        graphElem.appendChild(nodeElem);
      }

      for (int i=0 ; i<g.getHiddenNodeLayerSize() ; i++) {
        for (int j=0 ; j<g.getHiddenNodeNum(i) ; j++) {
          NNLayerNode n = g.getHiddenNode(i, j);
          Element nodeElem = createNodeElement(document, n);
          graphElem.appendChild(nodeElem);
        }
      }

      try {
        Transformer transformer = transformerFactory.newTransformer();

        transformer.setOutputProperty("indent", "yes");
        transformer.setOutputProperty("encoding", "Shift_JIS");

        transformer.transform(new DOMSource(document),
                new StreamResult(file));

      } catch (TransformerConfigurationException e) {
        throw new IOException(e);
      } catch (TransformerException e) {
        throw new IOException(e);
      }
    }

    private Element createNodeElement(Document document, NNLayerNode n) {
      Element nodeElem = document.createElement(NODE_TAG);
      nodeElem.setAttribute(ID_TAG, Integer.toString(n.hashCode()));
      nodeElem.setAttribute(CLASS_TAG, n.getClass().getName());

      NNNode[] inputs = n.getInputs();
      double[] weights = n.getWeights();
      for (int j=0 ; j<inputs.length ; j++) {
        NNNode child = inputs[j];
        Element childElem = document.createElement(NODE_TAG);
        String id = child==null
                ? ""
                : Integer.toString(child.hashCode());

        childElem.setAttribute(ID_TAG, id);
        childElem.setAttribute(WEIGHT_TAG, Double.toString(weights[j]));

        nodeElem.appendChild(childElem);
      }

      return nodeElem;
    }
  }

  private class GraphReader {
    private GraphReader() {}
    public Graph read() throws IOException {
      try {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();

        Node root = builder.parse(file);
        System.out.println(root);

        NodeList list = root.getChildNodes();

        for (int i=0 ; i<list.getLength() ; i++) {
          Node n = list.item(i);
          System.out.println(i + "," + n);

          NodeList list2 = n.getChildNodes();
          for (int j=0 ; j<list2.getLength() ; j++) {
            Node n2 = list2.item(j);
//            NamedNodeMap map2 = n2.getAttributes();
//            System.out.println(map2);
            System.out.println(i + "," + j + "=" + n2.getNodeName() + "," + n2.getNodeValue());
          }
        }

      } catch (ParserConfigurationException e) {
        throw new IOException(e);
      } catch (SAXException e) {
        throw new IOException(e);
      }

      return null;
    }
  }

  public static void main(String[] args) throws Exception {
    Graph g = new Graph(3, 3, 3);
    GraphWriter writer = GraphIO.getWriter("a.xml");
    writer.write(g);
    GraphReader reader = GraphIO.getReader("a.xml");
    System.out.println(reader.read());
  }
}