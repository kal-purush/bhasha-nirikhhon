package com.itheima.bos.fore.web.action;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.struts2.ServletActionContext;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.ParentPackage;
import org.apache.struts2.convention.annotation.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Controller;

import com.itheima.crm.domain.Customer;
import com.itheima.utils.MailUtils;
import com.itheima.utils.SmsUtils;
import com.opensymphony.xwork2.ActionContext;
import com.opensymphony.xwork2.ActionSupport;
import com.opensymphony.xwork2.ModelDriven;

import oracle.jdbc.util.Login;


/**  
 * ClassName:SignupAction <br/>  
 * Function:  <br/>  
 * Date:     2017年11月7日 下午4:50:04 <br/>       
 */

@Controller
@Namespace("/")
@ParentPackage("struts-default")
@Scope("prototype")
public class LoginAction extends ActionSupport implements ModelDriven<Customer> {
    private static final long serialVersionUID = 1L;
    
    private Customer model=new Customer();
    
    @Override
    public Customer getModel() {
        return model;
    }
    
    private String checkcode;
    
    
    public void setCheckcode(String checkcode) {
        this.checkcode = checkcode;
    }

    @Action(value="loginAction_login", 
            results ={@Result(name="success" ,location="index.html" ,type="redirect" ),
                      @Result(name="login" ,location="login.html" ,type="redirect" )})
    public String login(){
        HttpSession session = ServletActionContext.getRequest().getSession();
        String validateCode = (String) session.getAttribute("validateCode");
        if(validateCode!=null&&validateCode.equalsIgnoreCase(checkcode)){
            Customer customer = WebClient.create("http://localhost:8181/crm/webservice/customerService/login")
            .accept(MediaType.APPLICATION_JSON)
            .type(MediaType.APPLICATION_JSON)
            .query("telephone", model.getTelephone())
            .query("password", model.getPassword())
            .get(Customer.class);
            if(customer!=null){
                session.setAttribute("customer", customer);
                return SUCCESS; 
            }
            return LOGIN; 
        }
        return LOGIN;
    }
    
}