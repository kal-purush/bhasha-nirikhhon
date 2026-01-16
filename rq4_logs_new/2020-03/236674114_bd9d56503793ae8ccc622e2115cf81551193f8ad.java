
package gt.edu.usac.cunoc.ingenieria.eps.logout.view;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.inject.Named;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

@Named
@RequestScoped
public class LogoutView {
   @Inject
   private HttpServletRequest request;

   
   public String logout()
           throws ServletException {
       request.logout();
       request.getSession().invalidate();
       return "login";
   }
}