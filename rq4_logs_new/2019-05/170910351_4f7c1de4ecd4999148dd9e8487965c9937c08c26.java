package hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.exporting;

import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.Place;
import hse.se.aaizmaylov.petrinetslibrary.petrinets.basic.Transition;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.awt.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static hse.se.aaizmaylov.petrinetslibrary.utils.XmlUtils.getElementByTag;

public class PnmlColoredResult {
    private static final String GRAPHICS_TAG = "graphics";
    private static final String FILL_TAG = "fill";
    private static final String PLACE_TAG = "place";
    private static final String TRANSITION_TAG = "transition";
    private static final String COLOR_ATTR = "color";

    private Document pnmlDocument;

    public enum VertexColor {
        UNBOUND(new Color(255, 30, 33)),
        DEAD(new Color(0, 20, 255));

        VertexColor(Color color) {
            this.color = color;
        }

        @NotNull
        public String getHexString() {
            return "#" + Integer.toHexString(color.getRGB()).substring(2);
        }

        private Color color;
    }

    public PnmlColoredResult(@NonNull String pathToFile) throws ParserConfigurationException,
            IOException, SAXException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();

        pnmlDocument = builder.parse(new File(pathToFile));
    }

    public PnmlColoredResult markPlaceUnbounded(@NonNull Place place) {
        addColorToVertex(PLACE_TAG, place.label(), VertexColor.UNBOUND);

        return this;
    }

    public PnmlColoredResult markTransitionDead(@NonNull Transition transition) {
        addColorToVertex(TRANSITION_TAG, transition.label(), VertexColor.DEAD);

        return this;
    }

    public void saveToFile(@NonNull String fileName) throws TransformerException, IOException {
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");

        DOMSource source = new DOMSource(pnmlDocument);

        FileWriter writer = new FileWriter(new File(fileName));
        StreamResult result = new StreamResult(writer);

        transformer.transform(source, result);
    }

    private void addColorToVertex(String tag, String id, VertexColor color) {
        Element vertex = findVertexNode(tag, id);

        Element graphics = getElementByTag(vertex.getChildNodes(), GRAPHICS_TAG);
        if (graphics == null) {
            graphics = pnmlDocument.createElement(GRAPHICS_TAG);
            vertex.appendChild(graphics);
        }

        Element fill = getElementByTag(graphics.getChildNodes(), FILL_TAG);
        if (fill == null) {
            fill = pnmlDocument.createElement(FILL_TAG);
            graphics.appendChild(fill);
        }

        fill.setAttribute(COLOR_ATTR, color.getHexString());
    }

    private Element findVertexNode(String tag, String id) {
        NodeList list = pnmlDocument.getElementsByTagName(tag);

        for (int i = 0, length = list.getLength(); i < length; i++) {
            if (list.item(i).getAttributes().getNamedItem("id").getNodeValue().equals(id)) {
                return (Element) list.item(i);
            }
        }

        throw new UnexpectedVertexException("Cannot find " + tag + " with id " + id);
    }
}