package Search;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import beans.*;
import WebService.WebServiceCategorieProxy;
import WebService.WebServiceAnnonceProxy;

import java.io.IOException;
import java.io.StringReader;

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

public class SearchAnnonces extends HttpServlet {

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

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
		this.getServletContext().getRequestDispatcher("/search/index.jsp").forward(request, response);

	}

	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// Récupère les champs pour recherche
		String categorie_id_string = (String) request.getParameter("categorie_id");
		String annonce_nom = (String) request.getParameter("name");
		String ville = (String) request.getParameter("ville");
		String departement_string = (String) request.getParameter("departement");
		String est_recent_string = (String) request.getParameter("est_recent");

		// Transformation en int pour ceux doivent l'être
		int categorie_id = Integer.parseInt(categorie_id_string);
		
		int departement =0;
		if(departement_string != "")
			 departement = Integer.parseInt(departement_string);
		
		int est_recent_int = Integer.parseInt(est_recent_string);

		// Transformation en booléen du est_récent
		boolean est_recent = false;
		if (est_recent_int == 1)
			est_recent = true;
		else
			est_recent = false;
		
		System.out.println("Debug categorie : "+categorie_id);
		System.out.println("Debug nom : "+annonce_nom);
		System.out.println("Debug ville : "+ville);
		System.out.println("Debug departement : "+departement);
		System.out.println("Debug est_recent : "+est_recent);

		// Recherche
		WebServiceAnnonceProxy annonceProxy = new WebServiceAnnonceProxy();

		String annonces_resultat = annonceProxy.searchAnnonces(categorie_id, ville, annonce_nom, departement, est_recent);
		System.out.println("string " + annonces_resultat);

		// Tableau à envoyer à la JSP : id x nom x n° adresse x telephonne x
		// numero categorie x nom categorie
		ArrayList<Integer> id_list = new ArrayList<Integer>();
		ArrayList<String> nom_list = new ArrayList<String>();
		ArrayList<Integer> tel_list = new ArrayList<Integer>();
		ArrayList<Integer> adresse_id_list = new ArrayList<Integer>();
		ArrayList<Integer> categorie_id_list = new ArrayList<Integer>();
		ArrayList<String> categorie_nom_list = new ArrayList<String>();
		ArrayList<Integer> adresse_numero = new ArrayList<Integer>();
		ArrayList<String> adresse_rue = new ArrayList<String>();
		ArrayList<Integer> adresse_codePostal = new ArrayList<Integer>();
		ArrayList<String> adresse_ville = new ArrayList<String>();
		
		// Traitement XML de toutes les "categorie"
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = null;

		try {
			db = dbf.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		InputSource is = new InputSource();
		is.setCharacterStream(new StringReader(annonces_resultat));

		Document doc;
		try {
			doc = db.parse(is);
			System.out.println("teeest " + doc);

			// id des annonces
			NodeList nList_id = doc.getElementsByTagName("annonce_id");
			System.out.println("nombre d'id d'annonce :" + nList_id.getLength());

			for (int temp = 0; temp < nList_id.getLength(); temp++) {

				Node nNode = nList_id.item(temp);
				System.out.println("\nCurrent Element :" + nNode.getNodeName());

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					System.out.println("valeur de l'id : " + nNode.getTextContent());
					id_list.add((int) Long.valueOf(nNode.getTextContent()).longValue());

				}

			}
			// nom des annonces
			NodeList nList_nom = doc.getElementsByTagName("nom");
			System.out.println("nombre de name d'annonce :" + nList_nom.getLength());

			for (int temp = 0; temp < nList_nom.getLength(); temp++) {

				Node nNode = nList_nom.item(temp);
				System.out.println("\nCurrent Element :" + nNode.getNodeName());

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					System.out.println("valeur du nom : " + nNode.getTextContent());
					nom_list.add(nNode.getTextContent());

				}

			}

			// telephone des annonces
			NodeList nList_tel = doc.getElementsByTagName("telephone");
			System.out.println("nombre de telephone d'annonce :" + nList_tel.getLength());

			for (int temp = 0; temp < nList_tel.getLength(); temp++) {

				Node nNode = nList_tel.item(temp);
				System.out.println("\nCurrent Element :" + nNode.getNodeName());

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					System.out.println("valeur de telephone : " + nNode.getTextContent());
					tel_list.add((int) Long.valueOf(nNode.getTextContent()).longValue());

				}

			}

			// id des adresses
			NodeList nList_adresse_id = doc.getElementsByTagName("adresse_id");
			System.out.println("nombre d'id d'annonce :" + nList_adresse_id.getLength());

			for (int temp = 0; temp < nList_adresse_id.getLength(); temp++) {

				Node nNode = nList_adresse_id.item(temp);
				System.out.println("\nCurrent Element :" + nNode.getNodeName());

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					System.out.println("valeur de l'id de l'adresse: " + nNode.getTextContent());
					adresse_id_list.add((int) Long.valueOf(nNode.getTextContent()).longValue());

				}

			}

			// id et nom de la categorie des annonces
			NodeList nList_categorie_id = doc.getElementsByTagName("categorie");

			for (int temp = 0; temp < nList_categorie_id.getLength(); temp++) {

				Node nNode = nList_categorie_id.item(temp);

				System.out.println("\nCurrent Element :" + nNode.getNodeName());

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					Element eElement = (Element) nNode;

					System.out.println("Categorie id : " + eElement.getAttribute("id"));
					System.out.println("Categorie id : " + eElement.getAttribute("nom"));

					categorie_id_list.add(Integer.parseInt(eElement.getAttribute("id")));
					categorie_nom_list.add((eElement.getAttribute("nom")));

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
								adresse_numero.add(((int) Long.valueOf(nNode.getTextContent()).longValue()));

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
								adresse_rue.add(nNode.getTextContent());

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
								adresse_codePostal.add((int) Long.valueOf(nNode.getTextContent()).longValue());

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
								adresse_ville.add(nNode.getTextContent());

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
		request.setAttribute("liste_id", id_list);
		request.setAttribute("liste_noms", nom_list);
		request.setAttribute("liste_tels", tel_list);
		request.setAttribute("liste_adresse_id", adresse_id_list);
		request.setAttribute("liste_categorie_nom", categorie_nom_list);
		request.setAttribute("liste_categorie_id", categorie_id_list);
		
		request.setAttribute("liste_adresse_numero", adresse_numero);
		request.setAttribute("liste_adresse_rue", adresse_rue);
		request.setAttribute("liste_adresse_code_postal", adresse_codePostal);
		request.setAttribute("liste_adresse_ville", adresse_ville);

		System.out.println("out2?");
		this.getServletContext().getRequestDispatcher("/search/resultats.jsp").forward(request, response);
	}
}