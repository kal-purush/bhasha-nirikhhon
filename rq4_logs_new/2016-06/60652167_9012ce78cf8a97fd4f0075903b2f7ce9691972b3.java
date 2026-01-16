package WebService;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import DAO.AdresseDAO;
import DAO.AnnonceDAO;
import DAO.CategorieDAO;
import DAO.DAOFactory;
import beans.*;

public class WebServiceAnnonce {
	
	AnnonceDAO annonceDAO = (AnnonceDAO) DAOFactory.getAnnonceDAO();
	CategorieDAO categorieDAO = (CategorieDAO) DAOFactory.getCategorieDAO();
	AdresseDAO adresseDAO = (AdresseDAO) DAOFactory.getAdresseDAO();
	
	public WebServiceAnnonce() { super(); }
	
	
	//TODO DONE
	public String createAnnonce(String nom, int category_id, int telephone, int numero_adresse, String rue_adresse,  int codePostal_adresse, String ville_adresse ){
		
		System.out.println("le nom : "+nom);
		//Création d'un beans avant l'envoie à la DAO pour ajout BDD
		Annonce new_annonce = new Annonce();
		new_annonce.setNom(nom);
		new_annonce.setTelephone(telephone);
		new_annonce.setCategorie(categorieDAO.find(category_id));
		
		Adresse new_adresse = new Adresse();
		new_adresse.setId(0);
		new_adresse.setNumero(numero_adresse);
		new_adresse.setVille(ville_adresse);
		new_adresse.setRue(rue_adresse);
		new_adresse.setCodePostal(codePostal_adresse);
		
		new_annonce.setAdresse(new_adresse);
		
		//Ajout à la BDD
		long category_id_long = Long.parseLong(String.valueOf(category_id));
		Annonce annonce_adeed  = annonceDAO.create(new_annonce, category_id_long);
		
		//Contruction de la chaine XML de retour contenant l'annonce céée
		JAXBContext jaxbContext;
		try {
			jaxbContext = JAXBContext.newInstance(Annonce.class);
			java.io.StringWriter sw = new StringWriter();

		    Marshaller marshaller = jaxbContext.createMarshaller();
		    marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
		    marshaller.marshal(annonce_adeed, sw);

		    System.out.println("string XML added "+sw.toString());
		    return sw.toString();
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
	   return "false";
    	
	}
	
	
	//TODO DONE
	public String getAnnonces() {
		List<Annonce> l_annonces = annonceDAO.findAll();
		
		System.out.println(l_annonces.toString());
		System.out.println("Un ensemble de : " + l_annonces.size() + " annonces");
		for (int i = 0; i < l_annonces.size(); i++) {
			System.out.println("ID : "+l_annonces.get(i).getId()+" Nom : "+l_annonces.get(i).getNom()+", Categorie : "+l_annonces.get(i).getCategorie().getNom());
		}
		
		Annonces annonces = new Annonces();
		annonces.setAnnonces(l_annonces);
		
		JAXBContext jaxbContext;
		try {
			jaxbContext = JAXBContext.newInstance(Annonces.class);
			java.io.StringWriter sw2 = new StringWriter();

		    Marshaller marshaller = jaxbContext.createMarshaller();
		    marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
		    marshaller.marshal(annonces, sw2);

		    System.out.println("string XML annonce "+sw2.toString());
		    return sw2.toString();
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return "false";
    }
	
	//TODO DONE
	public String getAnnonceByID(int id) {
    	Annonce annonce = annonceDAO.find(id);

    	System.out.println("Categorie retournée : " + annonce.getId() + " // " + annonce.getNom());
		
		JAXBContext jaxbContext;
		try {
			jaxbContext = JAXBContext.newInstance(Annonce.class);
			java.io.StringWriter sw = new StringWriter();

		    Marshaller marshaller = jaxbContext.createMarshaller();
		    marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
		    marshaller.marshal(annonce, sw);

		    System.out.println("string XML "+sw.toString());
		    return sw.toString();
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
	   return "false";
    	
    }
	
	//TODO DONE
	public String getAnnoncesCat(int categorie_id) {
		List<Annonce> l_annonces_by_cat = annonceDAO.findAnnonceFromCategorie(categorie_id);
		
		System.out.println("Un ensemble de : " + l_annonces_by_cat.size() + " annonces");
		for (int i = 0; i < l_annonces_by_cat.size(); i++) {
			System.out.println("ID : "+l_annonces_by_cat.get(i).getId()+" Nom : "+l_annonces_by_cat.get(i).getNom()+", Categorie : "+l_annonces_by_cat.get(i).getCategorie().getNom());
		}
		
		Annonces annonces_by_cat = new Annonces();
		annonces_by_cat.setAnnonces(l_annonces_by_cat);
		
		JAXBContext jaxbContext;
		try {
			jaxbContext = JAXBContext.newInstance(Annonces.class);
			java.io.StringWriter sw2 = new StringWriter();

		    Marshaller marshaller = jaxbContext.createMarshaller();
		    marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
		    marshaller.marshal(annonces_by_cat, sw2);

		    System.out.println("string XML annonce by cat "+sw2.toString());
		    return sw2.toString();
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return "false";
	}
	
	//TODO 
	//MODIFY
public boolean modifyAnnonce(int annonce_id, String nom, int category_id, int telephone, int numero_adresse, String rue_adresse,  int codePostal_adresse, String ville_adresse ){
		
		System.out.println("le nom : "+nom);
		//Création d'un beans avant l'envoie à la DAO pour ajout BDD
		Annonce updated_annonce = new Annonce();
		updated_annonce.setId(annonce_id);
		updated_annonce.setNom(nom);
		updated_annonce.setTelephone(telephone);
		updated_annonce.setCategorie(categorieDAO.find(category_id));
		
		//Gestion nouvelle adresse ou pas
		Adresse updated_adresse = new Adresse();
		updated_adresse.setId(0);
		updated_adresse.setNumero(numero_adresse);
		updated_adresse.setVille(ville_adresse);
		updated_adresse.setRue(rue_adresse);
		updated_adresse.setCodePostal(codePostal_adresse);
		
		Adresse is_new_adresse = adresseDAO.find(numero_adresse, rue_adresse, ville_adresse, codePostal_adresse);
		
		//Adresse n'existe pas..A créer
		if(is_new_adresse == null){
			Adresse to_create = updated_adresse;
			Adresse new_adresse = adresseDAO.create(to_create);
			updated_annonce.setAdresse(new_adresse);
		}
			
		else{
			updated_annonce.setAdresse(is_new_adresse);
		}
		
		//Modif de l'adresse dans la BDD
		boolean success  = annonceDAO.modifyAnnonce(updated_annonce);
		
		return success;
    	
	}
	
	
	//TODO DONE
	public boolean deleteAnnonce(int id) {
		return annonceDAO.removeAnnonce(id);
	}
    

}