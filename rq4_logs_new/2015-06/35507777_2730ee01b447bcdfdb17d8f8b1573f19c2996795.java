package com.html5parser.parser;

import java.io.File;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.DocumentType;
import org.w3c.dom.Node;

import com.html5parser.classes.token.TagToken.Attribute;

public class Serializer {

	public static String toHtmlString(Node doc) {
		return toHtmlString(doc, true, false);
	}

	public static String toHtmlString(Node doc, boolean indent, boolean saveFile) {
		try {
			StringWriter writer = new StringWriter();
			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = tf.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
					"yes");
			transformer.setOutputProperty(OutputKeys.METHOD, "xml");
			transformer.setOutputProperty(OutputKeys.INDENT, indent ? "yes"
					: "no");
			transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			transformer.setOutputProperty(
					"{http://xml.apache.org/xslt}indent-amount", "4");

			if (saveFile) {
				transformer.transform(new DOMSource(doc), new StreamResult(
						new File("output.html")));
			}

			transformer.transform(new DOMSource(doc), new StreamResult(writer));
			return writer.toString();
		} catch (IllegalArgumentException
				| TransformerFactoryConfigurationError | TransformerException e) {
			e.printStackTrace();
			return null;

		}
	}

	public static String toHtml5libFormat(Node node) {
		String str = "";
		int ancestors = 0;
		if (node.getFirstChild() == null)
			return "| ";
		Node parent = node;
		Node current = node.getFirstChild();
		Node next = null;

		/*
		 * Check if there is an Invalid doctype
		 */
		if (parent.getUserData("invalidDoctype") != null)
			str += "\n| " + parent.getUserData("invalidDoctype").toString();

		for (;;) {
			str += "\n| " + indent(ancestors);
			switch (current.getNodeType()) {
			case Node.DOCUMENT_TYPE_NODE:
				String publicId = ((DocumentType) current).getPublicId();
				publicId = publicId == null ? "" : (" \"" + publicId + "\"");
				String systemId = ((DocumentType) current).getSystemId();
				if (systemId == null) {
					if (publicId.isEmpty())
						systemId = "";
					else
						systemId = " \"\"";
				} else
					systemId = " \"" + systemId + "\"";

				str += "<!DOCTYPE " + current.getNodeName() + publicId
						+ systemId + '>';
				break;
			case Node.COMMENT_NODE:
				try {
					str += "<!-- " + current.getNodeValue() + " -->";
				} catch (NullPointerException e) {
					str += "<!--  -->";
				}
				if (parent != current.getParentNode()) {
					return str += " (misnested... aborting)";
				}
				break;
			case 7:
				str += "<?" + current.getNodeName() + current.getNodeValue()
						+ '>';
				break;
			case Node.CDATA_SECTION_NODE:
				str += "<![CDATA[ " + current.getNodeValue() + " ]]>";
				break;
			case Node.TEXT_NODE:
				str += '"' + current.getNodeValue() + '"';
				if (parent != current.getParentNode()) {
					return str += " (misnested... aborting)";
				}
				break;
			case Node.ELEMENT_NODE:
				str += "<";
				if (current.getNamespaceURI() != null)
					switch (current.getNamespaceURI()) {
					case "http://www.w3.org/2000/svg":
						str += "svg ";
						break;
					case "http://www.w3.org/1998/Math/MathML":
						str += "math ";
						break;
					}
				// if (current.getNamespaceURI() != null
				// && current.getLocalName() != null) {
				// str += current.getLocalName();
				// } else {
				// str += current.getNodeName().toLowerCase();
				// }
				str += current.getNodeName();
				str += '>';
				if (parent != current.getParentNode()) {
					return str += " (misnested... aborting)";
				} else {
					@SuppressWarnings("unchecked")
					List<Attribute> invalidAtts = (List<Attribute>) current
							.getUserData("invalidAtts");

					if (current.hasAttributes() || invalidAtts != null) {
						// List<String> attrNames = new ArrayList<String>();
						// Map<String, Integer> attrPos = new HashMap<String,
						// Integer>();

						Map<String, String> attrNames = new HashMap<String, String>();

						for (int j = 0; j < current.getAttributes().getLength(); j += 1) {
							if (current.getAttributes().item(j) != null) {
								String name = "";
								if (current.getAttributes().item(j)
										.getNamespaceURI() != null)
									switch (current.getAttributes().item(j)
											.getNamespaceURI()) {
									case "http://www.w3.org/XML/1998/namespace":
										name += "xml ";
										break;
									case "http://www.w3.org/2000/xmlns/":
										name += "xmlns ";
										break;
									case "http://www.w3.org/1999/xlink":
										name += "xlink ";
										break;
									}
								if (current.getAttributes().item(j)
										.getLocalName() != null) {
									name += current.getAttributes().item(j)
											.getLocalName();
								} else {
									name += current.getAttributes().item(j)
											.getNodeName();
								}
								// attrNames.add(name);
								// attrPos.put(name, j);
								attrNames.put(name, current.getAttributes()
										.item(j).getNodeValue());
							}
						}

						/*
						 * Check if there are invalid attributes
						 */
						if (invalidAtts != null) {
							for (int j = 0; j < invalidAtts.size(); j += 1) {
								if (invalidAtts.get(j) != null) {
									String name = "";
									if (invalidAtts.get(j).getNamespace() != null)
										switch (invalidAtts.get(j)
												.getNamespace()) {
										case "http://www.w3.org/XML/1998/namespace":
											name += "xml ";
											break;
										case "http://www.w3.org/2000/xmlns/":
											name += "xmlns ";
											break;
										case "http://www.w3.org/1999/xlink":
											name += "xlink ";
											break;
										}
									if (invalidAtts.get(j).getLocalName() != null) {
										name += invalidAtts.get(j)
												.getLocalName();
									} else {
										name += invalidAtts.get(j).getName();
									}
									// attrNames.add(name);
									// attrPos.put(name, j);
									attrNames.put(name, invalidAtts.get(j)
											.getValue());
								}
							}
						}

						// if (attrNames.size() > 0) {
						// attrNames.sort(null);
						// for (int j = 0; j < attrNames.size(); j += 1) {
						// str += "\n| " + indent(1 + ancestors)
						// + attrNames.get(j);
						// str += "=\""
						// + current
						// .getAttributes()
						// .item(attrPos.get(attrNames
						// .get(j)))
						// .getNodeValue() + "\"";
						// }
						// }

						if (attrNames.size() > 0) {
							TreeMap<String, String> sorted_map = new TreeMap<String, String>(
									attrNames);
							for (Map.Entry<String, String> entry : sorted_map
									.entrySet()) {
								String key = entry.getKey();
								String value = entry.getValue();

								str += "\n| " + indent(1 + ancestors) + key;
								str += "=\"" + value + "\"";
							}
						}
					}
					next = current.getFirstChild();
					if (null != next) {
						parent = current;
						current = next;
						ancestors++;
						continue;
					}
				}
				break;
			}
			for (;;) {
				next = current.getNextSibling();
				if (next != null) {
					current = next;
					break;
				}
				current = current.getParentNode();
				parent = parent.getParentNode();
				ancestors--;
				if (current == node) {
					return str.substring(1);
				}
			}
		}
	}

	private static String indent(int ancestors) {
		String str = "";
		if (ancestors > 0) {
			while (0 <= --ancestors)
				str += "  ";
		}
		return str;
	}
}