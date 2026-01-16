package WebService;
import java.util.List;
import beans.Categorie;
import DAO.CategorieDAO;
import DAO.DAOFactory;



public class WebServiceCategorie {
	
	CategorieDAO categorieDAO = (CategorieDAO) DAOFactory.getCategorieDAO();
	
	public WebServiceCategorie() { super(); }
	
	public Categorie getCategorie(int categorie_id) {
		Categorie categorie = categorieDAO.find(categorie_id);
		System.out.println("Categorie retournée : " + categorie.getId() + " // " + categorie.getNom());
		return categorie;
	}
	
	public Categorie[] getCategories() {
		List<Categorie> l_categories = categorieDAO.findAll();
		System.out.println("Un ensemble de : " + l_categories.size() + " categories");
		for (int i = 0; i < l_categories.size(); i++) {
			System.out.println("ID : "+l_categories.get(i).getId()+" Nom : "+l_categories.get(i).getNom());
		}
		return l_categories.toArray(new Categorie[l_categories.size()]);
	}
	
	public Categorie newCategorie(String nom) {
		System.out.println("Categorie ajoutée !");
		System.out.println("Categorie nom :"+nom);
		
		Categorie cat = new Categorie(0, nom);
		return categorieDAO.create(cat);
	}
	
	public boolean delCategorie(int categorie_id) {
		System.out.println("Categorie  "+categorie_id+" supprimée !");
		return categorieDAO.remove(categorie_id);
	}
	
	public boolean modifyCategorie(int id, String nom) {
		Categorie categorie = new Categorie();
		categorie.setId(id);
		categorie.setNom(nom);
		System.out.println("Categorie  "+categorie.getId()+" modifée !");
		return categorieDAO.modify(categorie);
	}
	
}
