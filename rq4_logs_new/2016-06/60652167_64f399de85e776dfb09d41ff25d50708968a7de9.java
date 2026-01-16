package beans;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
 
@XmlRootElement(name = "categories")
@XmlAccessorType (XmlAccessType.FIELD)
public class Categories 
{
    @XmlElement(name = "categorie")
    private List<Categorie> categories = null;
 
    public List<Categorie> getCategories() {
        return categories;
    }
 
    public void setEmployees(List<Categorie> categories) {
        this.categories = categories;
    }
}