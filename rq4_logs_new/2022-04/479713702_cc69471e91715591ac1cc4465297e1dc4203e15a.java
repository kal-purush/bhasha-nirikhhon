
package converter;
import dao.UserDao;
import entity.User;
import jakarta.faces.component.UIComponent;
import jakarta.faces.context.FacesContext;
import jakarta.faces.convert.Converter;
import jakarta.faces.convert.FacesConverter;

    
  @FacesConverter("userConverter")
public class UserConverter implements Converter{

    private UserDao userDao;

    public UserDao getUserDao() {
        if(this.userDao == null){
            userDao = new UserDao();
        }
        return userDao;
    }

    public void setUserDao(UserDao userDao) {
        this.userDao = userDao;
    }
    
    
    @Override
    public Object getAsObject(FacesContext fc, UIComponent uic, String string) {
           int id= Integer.valueOf(string);
           User c = this.getUserDao().findById(id);
           return c;
    }

    @Override
    public String getAsString(FacesContext fc, UIComponent uic, Object t) {
           User c = (User) t;
           return String.valueOf(c.getId());

    }
    
}  
    
    
