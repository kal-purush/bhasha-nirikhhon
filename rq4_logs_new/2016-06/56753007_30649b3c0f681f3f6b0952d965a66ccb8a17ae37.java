/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.project.ecampusconnect.department;

/**
 *
 * @author Advait
 */
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.ejb.LocalBean;
import javax.ejb.Singleton;

@Singleton
@LocalBean
public class Department 
{
    private Map<String, String> services;
    
    public Department()
    {
        
    }
    
    @PostConstruct
    public void init(){
    	services = new HashMap<String, String>();
    	services.put("Computer Science", "Web Application");
    	services.put("Information Systems", "Campus Connect");
    }

	public Map<String, String> getServices() {
		return services;
	}
}