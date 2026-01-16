package converter;

import dao.CategoryDao;
import entity.Category;
import jakarta.faces.component.UIComponent;
import jakarta.faces.context.FacesContext;
import jakarta.faces.convert.Converter;
import jakarta.faces.convert.FacesConverter;

@FacesConverter("categoryConverter")
public class CategoryConverter implements Converter{

    private CategoryDao categoryDao;

    public CategoryDao getCategoryDao() {
        if(this.categoryDao == null){
            categoryDao = new CategoryDao();
        }
        return categoryDao;
    }

    public void setCategoryDao(CategoryDao categoryDao) {
        this.categoryDao = categoryDao;
    }
    @Override
    public Object getAsObject(FacesContext fc, UIComponent uic, String string) {
           int id= Integer.valueOf(string);
           Category c = this.getCategoryDao().findById(id);
           return c;
    }

    @Override
    public String getAsString(FacesContext fc, UIComponent uic, Object t) {
           Category c = (Category) t;
           return String.valueOf(c.getId());

    }
    
}