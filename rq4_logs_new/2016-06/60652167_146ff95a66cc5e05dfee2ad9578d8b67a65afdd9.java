package AnnonceGestion;

import WebService.WebServiceCategorieProxy;
import WebService.WebServiceAnnonceProxy;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class Editing extends HttpServlet {

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		WebServiceAnnonceProxy annonceProxy = new WebServiceAnnonceProxy();

		//Deux façon d'arriver sur cette page : depuis la page de liste des annonces ou depuis la soumission du formulaire de cette même page
		String annonce_id_string = (String) request.getParameter("annonce_id");
		System.out.println(annonce_id_string);
		int annonce_id = 0;
		if(annonce_id_string == null){
			int annonce_id_attribute = (int) request.getAttribute("annonce_id");
			annonce_id = (int) Long.valueOf(annonce_id_attribute).longValue();
		}
		else {
			annonce_id = (int) Long.valueOf(annonce_id_string).longValue();
		}
		

		
		// Récupère la categorie à modifier
		String annonce_XML = annonceProxy.getAnnonceByID(annonce_id);
		System.out.println("String XML annonce à afficher : " + annonce_XML);

		// Paramètres à renvoyer à la JSP :
		int annonce_id_JSP = 0;
		String annonce_nom_JSP = null;
		int annonce_telephone_JSP = 0;
		int adresse_numero_JSP = 0;
		String adresse_rue_JSP = null;
		int adresse_code_postale_JSP = 0;
		String adresse_ville_JSP = null;
		int categorie_id_JSP = 0;
		String categorie_nom_JSP = null;

		// Traitement XML de la chaine reçu (une categorie)
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = null;

		try {
			db = dbf.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		InputSource is = new InputSource();
		is.setCharacterStream(new StringReader(annonce_XML));

		Document doc;
		try {
			doc = db.parse(is);
			System.out.println("teeest " + doc);

			// id de l'annonce
			NodeList nList_id = doc.getElementsByTagName("annonce_id");
			System.out.println("nombre d'id d'annonce :" + nList_id.getLength());

			for (int temp = 0; temp < nList_id.getLength(); temp++) {

				Node nNode = nList_id.item(temp);
				System.out.println("\nCurrent Element :" + nNode.getNodeName());

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					System.out.println("valeur de l'id : " + nNode.getTextContent());
					annonce_id_JSP = ((int) Long.valueOf(nNode.getTextContent()).longValue());

				}

			}
			// nom de l'annonce
			NodeList nList_nom = doc.getElementsByTagName("nom");
			System.out.println("nombre de name d'annonce :" + nList_nom.getLength());

			for (int temp = 0; temp < nList_nom.getLength(); temp++) {

				Node nNode = nList_nom.item(temp);
				System.out.println("\nCurrent Element :" + nNode.getNodeName());

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					System.out.println("valeur du nom : " + nNode.getTextContent());
					annonce_nom_JSP = (nNode.getTextContent());

				}

			}

			// telephone de l'annonce
			NodeList nList_tel = doc.getElementsByTagName("telephone");
			System.out.println("nombre de telephone d'annonce :" + nList_tel.getLength());

			for (int temp = 0; temp < nList_tel.getLength(); temp++) {

				Node nNode = nList_tel.item(temp);
				System.out.println("\nCurrent Element :" + nNode.getNodeName());

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					System.out.println("valeur de telephone : " + nNode.getTextContent());
					annonce_telephone_JSP = ((int) Long.valueOf(nNode.getTextContent()).longValue());

				}

			}

			// numero de l'adresse
			NodeList nList_adresse_numero = doc.getElementsByTagName("numero");
			System.out.println("nombre d'id d'annonce :" + nList_adresse_numero.getLength());

			for (int temp = 0; temp < nList_adresse_numero.getLength(); temp++) {

				Node nNode = nList_adresse_numero.item(temp);
				System.out.println("\nCurrent Element :" + nNode.getNodeName());

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					System.out.println("numero addr: " + nNode.getTextContent());
					adresse_numero_JSP = ((int) Long.valueOf(nNode.getTextContent()).longValue());

				}

			}

			// rue de l'adresse
			NodeList nList_adresse_rue = doc.getElementsByTagName("rue");
			System.out.println("nombre de name d'annonce :" + nList_adresse_rue.getLength());

			for (int temp = 0; temp < nList_adresse_rue.getLength(); temp++) {

				Node nNode = nList_adresse_rue.item(temp);
				System.out.println("\nCurrent Element :" + nNode.getNodeName());

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					System.out.println("rue addr: " + nNode.getTextContent());
					adresse_rue_JSP = (nNode.getTextContent());

				}

			}

			// code postal de l'adresse
			NodeList nList_adresse_code_postale = doc.getElementsByTagName("codePostal");
			System.out.println("nombre d'id d'annonce :" + nList_adresse_code_postale.getLength());

			for (int temp = 0; temp < nList_adresse_code_postale.getLength(); temp++) {

				Node nNode = nList_adresse_code_postale.item(temp);
				System.out.println("\nCurrent Element :" + nNode.getNodeName());

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					System.out.println("CP addr: " + nNode.getTextContent());
					adresse_code_postale_JSP = ((int) Long.valueOf(nNode.getTextContent()).longValue());

				}

			}

			// ville de l'adresse
			NodeList nList_adresse_ville = doc.getElementsByTagName("ville");
			System.out.println("nombre de name d'annonce :" + nList_adresse_ville.getLength());

			for (int temp = 0; temp < nList_adresse_ville.getLength(); temp++) {

				Node nNode = nList_adresse_ville.item(temp);
				System.out.println("\nCurrent Element :" + nNode.getNodeName());

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					System.out.println("ville addr: " + nNode.getTextContent());
					adresse_ville_JSP = (nNode.getTextContent());

				}

			}

			// id de la categorie de l'annonce
			NodeList nList_categorie_id = doc.getElementsByTagName("categorie");

			for (int temp = 0; temp < nList_categorie_id.getLength(); temp++) {

				Node nNode = nList_categorie_id.item(temp);

				System.out.println("\nCurrent Element :" + nNode.getNodeName());

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					Element eElement = (Element) nNode;

					System.out.println("Categorie id : " + eElement.getAttribute("id"));
					System.out.println("Categorie id : " + eElement.getAttribute("nom"));
					categorie_id_JSP = Integer.parseInt(eElement.getAttribute("id"));
					categorie_nom_JSP = eElement.getAttribute("nom");

				}
			}
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Récupération des categories et de leurs id
		WebServiceCategorieProxy categorieProxy = new WebServiceCategorieProxy();

		String categories = categorieProxy.getCategories();
		System.out.println("string " + categories);

		// Tableau à envoyer à la JSP : id x nom
		HashMap<Integer, String> categories_array = new HashMap<Integer, String>();

		// Traitement XML de toutes les "categorie"
		DocumentBuilderFactory dbf_1 = DocumentBuilderFactory.newInstance();
		DocumentBuilder db_1 = null;

		try {
			db_1 = dbf_1.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		InputSource is_1 = new InputSource();
		is_1.setCharacterStream(new StringReader(categories));

		Document doc_1;
		try {
			doc_1 = db_1.parse(is_1);
			System.out.println("teeest " + doc_1);
			NodeList nList = doc_1.getElementsByTagName("categorie");
			System.out.println(nList.getLength());

			for (int temp = 0; temp < nList.getLength(); temp++) {

				Node nNode = nList.item(temp);

				System.out.println("\nCurrent Element :" + nNode.getNodeName());

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					Element eElement = (Element) nNode;

					System.out.println("Categorie id : " + eElement.getAttribute("id"));
					System.out.println("Categorie id : " + eElement.getAttribute("nom"));
					categories_array.put(Integer.parseInt(eElement.getAttribute("id")), eElement.getAttribute("nom"));
				}
			}
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Envoie la hasMap à la JSP
		request.setAttribute("liste_categories", categories_array);

		System.out.println("out?");
		// Envoie la hasMap à la JSP

		request.setAttribute("annonce_id", annonce_id_JSP);
		request.setAttribute("annonce_nom", annonce_nom_JSP);
		request.setAttribute("annonce_telephone", annonce_telephone_JSP);
		request.setAttribute("adresse_numero", adresse_numero_JSP);
		request.setAttribute("adresse_rue", adresse_rue_JSP);
		request.setAttribute("adresse_codePostal", adresse_code_postale_JSP);
		request.setAttribute("adresse_ville", adresse_ville_JSP);
		request.setAttribute("categorie_id", categorie_id_JSP);
		request.setAttribute("categorie_nom", categorie_nom_JSP);

		this.getServletContext().getRequestDispatcher("/annonce/details.jsp").forward(request, response);
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// Récupère les champs pour ajout
		String nouveau_nom = (String) request.getParameter("nom");
		String nouveau_categorie_id_string = (String) request.getParameter("categorie_id");
		String nouveau_telephone_string = (String) request.getParameter("telephone");
		String nouveau_numero_string = (String) request.getParameter("numero");
		String nouveau_rue = (String) request.getParameter("rue");
		String nouveau_code_postale_string = (String) request.getParameter("code_postale");
		String nouveau_ville = (String) request.getParameter("ville");
		
		String annonce_id_string = (String) request.getParameter("annonce_id");
		

		// Transformation en int pour ceux doivent l'être
		int nouveau_categorie_id = Integer.parseInt(nouveau_categorie_id_string);
		int nouveau_telephone = Integer.parseInt(nouveau_telephone_string);
		int nouveau_numero = Integer.parseInt(nouveau_numero_string);
		int nouveau_code_postale = Integer.parseInt(nouveau_code_postale_string);
		int annonce_id = Integer.parseInt(annonce_id_string);

		// UPDATE
		WebServiceAnnonceProxy annonceProxy = new WebServiceAnnonceProxy();

		// Requete le webservice pour modifier une annonce
		annonceProxy.modifyAnnonce(annonce_id, nouveau_nom, nouveau_categorie_id, nouveau_telephone, nouveau_numero, nouveau_rue, nouveau_code_postale, nouveau_ville);

		
		request.setAttribute("annonce_id", annonce_id_string);
		
		//Récupère toutes les annonces pour les afficher
		doGet(request, response);

	}
}