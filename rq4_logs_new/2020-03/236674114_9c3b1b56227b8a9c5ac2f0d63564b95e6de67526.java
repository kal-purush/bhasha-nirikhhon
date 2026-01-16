package gt.edu.usac.cunoc.ingenieria.eps.user.converter;

import gt.edu.usac.cunoc.ingenieria.eps.user.Career;
import gt.edu.usac.cunoc.ingenieria.eps.user.UserCareer;
import gt.edu.usac.cunoc.ingenieria.eps.user.facade.UserFacadeLocal;
import javax.ejb.EJB;
import javax.faces.component.UIComponent;
import javax.faces.context.FacesContext;
import javax.faces.convert.Converter;
import javax.faces.convert.FacesConverter;


@FacesConverter(value = "careerConverter", managed = true)
public class CareerConverter implements Converter {
    @EJB
    private UserFacadeLocal userFacade;
    
    @Override
    public UserCareer getAsObject(FacesContext context, UIComponent component, String careerId) {
        try {
            System.out.println("id----------"+careerId);
            //Career career = userFacade.getCareer(new Career(Integer.parseInt(careerId),null)).get(0);
            UserCareer userCareer= userFacade.getUserCareer(new UserCareer(Integer.parseInt(careerId))).get(0);
            System.out.println("career------------------"+userCareer.getCAREERcodigo().getName());
            return userCareer;              
        } catch (Exception e) {
            System.out.println("----========------- Error Converter Career");
            return null; 
        }
}

    @Override
    public String getAsString(FacesContext context, UIComponent component, Object userCareer) {
        return ((UserCareer) userCareer).getId().toString();
    }

}