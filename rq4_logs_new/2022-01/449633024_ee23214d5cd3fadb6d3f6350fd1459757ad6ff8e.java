	package controller;

import java.text.ParseException;

import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import com.google.gson.Gson;

import entities.EmployeeDetail;
import info.AdInfo;
import th.in.totemplate.core.authen.ActiveDirectory;
import th.in.totemplate.core.authen.AuthenAttributes;

@Controller
public class LoginController { 
//	private String wrongUP = "Username	 or Password is incorrect."; 
	private String alertmsgText = "";
	private String alerttypText = "";
	public LoginController(HttpSession session) throws ParseException { } 
	@RequestMapping(value = { "/login" }, method = { RequestMethod.GET })
	public String getLoginModel(Model  model, HttpSession session, HttpServletRequest request, 
			HttpServletResponse response ) { 
		model.addAttribute("alertmsg", alertmsgText);
		model.addAttribute("alerttyp", alerttypText); 
		return "login";
	} 
	@RequestMapping(value = { "/logout" }, method = { RequestMethod.GET })
	public 	String getLogout(HttpSession session, HttpServletRequest req) { 
//		HttpSession ses = req.getSession(false);
	     if(session != null) {
	         session.removeAttribute("user");  
		     session.removeAttribute("userName");  
		     session.removeAttribute("userObject");  
		     alertmsgText = "";
		     alerttypText = "";
	     }
		return "redirect:/login";
	}
//	@RequestMapping(value = { "/login" }, method = { RequestMethod.GET })
//	public ModelAndView getLoginModel(Model  model, HttpSession session, HttpServletRequest request, 
//			HttpServletResponse response, String errormsg) { 
//		ModelAndView mv = new ModelAndView();   
//		mv.setViewName("login");    
//		return mv;
//	} 
//	@GetMapping(value = { "/login/loginAuth" } ) 
	@RequestMapping(value = { "/login/loginAuth" }, method = { RequestMethod.POST }) 
	public String getLoginAuthen(Model model, final String userId, String userPassword, final HttpSession session,
			RedirectAttributes redirectAttributes,HttpServletRequest request, 
			HttpServletResponse response) { 
		final Gson g = new Gson(); 
		final EmployeeDetail user = new EmployeeDetail( );	
		final TempLogin temp = new TempLogin();
		String redirect = "";
//		String userName = "";
		if(session.getAttribute("user") != null) { 
			alertmsgText = "";
			alerttypText ="";
			redirect = "redirect:/PCMSMain" ;  
			return redirect;  
//			user = (EmployeeDetail)session.getAttribute("user");
		}
		else {  
			if(userId.equals("test")) {
				user.setFirstName("Test");
				user.setEmployeeID(userId);      
				user.setPassword(userPassword);
				temp.setStatus(true); 
             	session.setAttribute("user", userId);       
 				session.setAttribute("userObject", g.toJson(user));  
			}
			else { 
				user.setEmployeeID(userId);
				user.setPassword(userPassword);
		        try {  
	                AdInfo info = AdInfo.getInstance();
	                ActiveDirectory.getAttributes(
	                    ActiveDirectory.getContext(
	                        info,
	                        user.getEmployeeID(),
	                        user.getPassword()),
		                    info.getSearchName(),
		                    info.getSearchFilter(user.getEmployeeID()),
		                    new AuthenAttributes() {
		                        @Override
		                        public void set(Attributes attribute) {
		                            try {  
		                                user.setDepartment( ((attribute.get("department") == null) ? "" : attribute.get("department").get().toString()) );
		                                user.setFirstName(  ((attribute.get("givenName")  == null) ? "" : attribute.get("givenName").get().toString())  );
		                                user.setLastName(   ((attribute.get("sn")         == null) ? "" : attribute.get("sn").get().toString())         );
		                                user.setEmail(      ((attribute.get("mail")       == null) ? "" : attribute.get("mail").get().toString())       );
		                                temp.setStatus(true); 
		                             	session.setAttribute("user", userId);      
		                 				session.setAttribute("userObject", g.toJson(user));  
		                            } catch(NamingException e) { e.printStackTrace(); }
		                        }
		                    }
		                ); 
		        } catch(NamingException e) {
		            System.err.println(this.getClass().getName()+" - "+e.getMessage());
		            e.printStackTrace();
		        }   
			}
		} 
		    
//		   System.out.println(session.getAttribute("user"));
//		   System.out.println(user.getFirstName());
//		   System.out.println(temp.getStatus());
		if(user.getFirstName() != null) {
			alertmsgText = "";
			alerttypText ="";
			redirect = "redirect:/PCMSMain" ;  
			return redirect;  
		 } 
		else {
			redirect = "redirect:/login" ;  
//			redirect = "redirect:login" ;       
			 if(temp.getStatus()) {
				 alertmsgText = "Unauthorized Access Prohibited";
				 alerttypText = "error";
//	        	model.addAttribute("alerttyp",  "error"  );   
//	        	model.addAttribute("alertmsg", "Username or Password is incorrect."  );  
//	        	System.out.println("hi1"); 
			 }
			else { 
				alertmsgText = "Username or Password is incorrect";
				alerttypText = "warning";
//	            model.addObject("alerttyp", g.toJson("warning") );   
//	        	model.addObject("alertmsg", g.toJson("Username or Password is incorrect") );   
//	        	model.addAttribute("alerttyp", "warning"  );   
//	        	model.addAttribute("alertmsg", "Username or Password is incorrect"  );   
	        }
		}  
		return redirect;  
	}  
	class TempLogin {
	    private boolean status;
	    public TempLogin() { this.status = false; } 
	    public void setStatus(boolean status) { this.status = status; }
	    public boolean getStatus() { return this.status; }
	} 
}