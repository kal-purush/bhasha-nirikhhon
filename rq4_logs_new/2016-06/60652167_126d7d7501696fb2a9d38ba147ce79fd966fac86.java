package AnnonceAjout;

import java.util.HashMap;

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

/**
 * <br>
 * Servlet d'affichage et d'ajout d'annonces.</b>
 * <p>
 * Gestion des réponses aux appels GET et POST
 * </p>
 * 
 * 
 * @author Gabriel Etienne
 * @version 1.2
 */
@SuppressWarnings("serial")
public class Adding extends HttpServlet {

	/**
	 * Méthode de la servlet permettant la gestion de requête GET
	 * 
	 * Formulaire d'ajout d'une nnoance
	 * 
	 * Récupération de la liste des catégories disponibles pour le formulaire
	 * d'ajout d'annonce
	 * 
	 * Traitement du XML pour le renvoie des informations servant à l'affichage
	 * à la JSP
	 * 
	 * @see WebService.WebServiceCategorieProxy
	 * 
	 */
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


		this.getServletContext().getRequestDispatcher("/annonce/ajout.jsp").forward(request, response);

	}

	/**
	 * Méthode de la servlet permettant la gestion de requête POST
	 * 
	 * Récupération de l'annonce à ajouter. Envoie des informations d'ajout
	 * d'annonce à l'annonce proxy qui va requêter le webservice.
	 * 
	 * @see WebService.WebServiceAnnonceProxy
	 * @see WebService.WebServiceCategorieProxy
	 * 
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// Récupère les champs pour ajout
		String nouveau_nom = (String) request.getParameter("nom");
		String nouveau_categorie_id_string = (String) request.getParameter("categorie_id");
		String nouveau_telephone_string = (String) request.getParameter("telephone");
		String nouveau_numero_string = (String) request.getParameter("numero");
		String nouveau_rue = (String) request.getParameter("rue");
		String nouveau_code_postal_string = (String) request.getParameter("code_postal");
		String nouveau_ville = (String) request.getParameter("ville");

		// Transformation en int pour ceux doivent l'être
		int nouveau_categorie_id = Integer.parseInt(nouveau_categorie_id_string);
		int nouveau_telephone = Integer.parseInt(nouveau_telephone_string);
		int nouveau_numero = Integer.parseInt(nouveau_numero_string);
		int nouveau_code_postal = Integer.parseInt(nouveau_code_postal_string);
		
		System.out.println("nom : "+nouveau_nom);
		System.out.println("cat : "+nouveau_categorie_id);
		System.out.println("tel : "+nouveau_telephone);
		System.out.println("n° : "+nouveau_numero);
		System.out.println("rue : "+nouveau_rue);
		System.out.println("cp : "+nouveau_code_postal);
		System.out.println("ville : "+nouveau_ville);
		

		// AJOUT
		WebServiceAnnonceProxy annonceProxy = new WebServiceAnnonceProxy();
		System.out.println(annonceProxy.createAnnonce(nouveau_nom, nouveau_categorie_id, nouveau_telephone, nouveau_numero, nouveau_rue,
				nouveau_code_postal, nouveau_ville));

		// Récupère toutes les annonces pour les afficher
		this.getServletContext().getRequestDispatcher("/annonce/ajoutSucces.jsp").forward(request, response);
	}
}