package converter;

import dao.BlogDao;
import entity.Blog;
import jakarta.faces.component.UIComponent;
import jakarta.faces.context.FacesContext;
import jakarta.faces.convert.Converter;
import jakarta.faces.convert.FacesConverter;


@FacesConverter("BlogConverter")
public class BlogConverter implements Converter{

    private BlogDao blogDao;

    public BlogDao getBlogDao() {
        if(this.blogDao == null){
            blogDao = new BlogDao();
        }
        return blogDao;
    }

    public void setBlogDao(BlogDao blogDao) {
        this.blogDao = blogDao;
    }
    
   
    @Override
    public Object getAsObject(FacesContext fc, UIComponent uic, String string) {
           int id= Integer.valueOf(string);
           Blog c = this.getBlogDao().findById(id);
           return c;
    }

    @Override
    public String getAsString(FacesContext fc, UIComponent uic, Object t) {
           Blog c = (Blog) t;
           return String.valueOf(c.getId());

    }
    
}