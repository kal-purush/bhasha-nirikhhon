package fr.iutbay.joey.TP7;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class XpathOfTheDeath
{
	public static void main(String[] args)
	{
		try
		{
			// Pour commencer, l'application r�cup�re un constructeur de documents
			// (javax.xml.parsers.DocumentBuilder) � partir d'une fabrique de
			// constructeurs (javax.xml.parsers.DocumentBuilderFactory).
			// C'est ce constructeur de documents qui va construire le
			// DOM ou document (org.w3c.Document) � partir de la source XML.
			// Le Document est la repr�sentation, sous forme d'arbre "d'objets", des donn�es
			// contenues dans le XML.
			// cr�ation d'une fabrique de documents
			DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
			// cr�ation d'un constructeur de documents
			DocumentBuilder builder = builderFactory.newDocumentBuilder();
			// lecture du contenu d'un fichier XML avec DOM
			FileInputStream file = new FileInputStream(new File("Employe.xml"));
			Document xmlDocument = builder.parse(file);
			// cr�ation du XPath
			XPath xPath = XPathFactory.newInstance().newXPath();
			// question a
			System.out.println("*a************************");
			String expression = "/employes/employe[@id='3333']/email";
			System.out.println(expression);
			// �valuation de l'expression XPath
			String email = xPath.compile(expression).evaluate(xmlDocument);
			System.out.println(email);
			// question b
			System.out.println("*b************************");
			expression = "/employes/employe/prenom";
			System.out.println(expression);
			NodeList nodeList = (NodeList) xPath.compile(expression).evaluate(xmlDocument, XPathConstants.NODESET);
			for(int i = 0; i < nodeList.getLength(); i++)
			{
				System.out.println(nodeList.item(i).getFirstChild().getNodeValue());
			}
			
			// question c
			System.out.println("*c************************");
			expression = "/employes/employe[@type='admin']/prenom";
			System.out.println(expression);
			nodeList = (NodeList) xPath.compile(expression).evaluate(xmlDocument, XPathConstants.NODESET);
			for(int i = 0; i < nodeList.getLength(); i++)
			{
				System.out.println(nodeList.item(i).getFirstChild().getNodeValue());
			}
	
			
			
			
			// question d
			System.out.println("*d************************");
			expression = "/employes/employe[age>40]/prenom";
			System.out.println(expression);
			nodeList = (NodeList) xPath.compile(expression).evaluate(xmlDocument, XPathConstants.NODESET);
			for(int i = 0; i < nodeList.getLength(); i++)
			{
				System.out.println(nodeList.item(i).getFirstChild().getNodeValue());
			}
			
			
			
			
			// question e
			System.out.println("*e************************");
			expression = "/employes/employe[position()<3]/prenom";
			System.out.println(expression);
			nodeList = (NodeList) xPath.compile(expression).evaluate(xmlDocument, XPathConstants.NODESET);
			for(int i = 0; i < nodeList.getLength(); i++)
			{
					System.out.println(nodeList.item(i).getFirstChild().getNodeValue());
			}
			
			// question f
			System.out.println("*f************************");
			expression = "/employes/employe[last()]/prenom";
			System.out.println(expression);
			nodeList = (NodeList) xPath.compile(expression).evaluate(xmlDocument, XPathConstants.NODESET);
			for(int i = 0; i < nodeList.getLength(); i++)
			{
					System.out.println(nodeList.item(i).getFirstChild().getNodeValue());
			}
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
		catch (SAXException e)
		{
			e.printStackTrace();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		catch (ParserConfigurationException e)
		{
			e.printStackTrace();
		}
		catch (XPathExpressionException e)
		{
			e.printStackTrace();
		}
	}
}