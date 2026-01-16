/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.project.ecampusconnect.main;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

/**
 *
 * @author Advait
 */
public abstract class SessionBean 
{
    private final String persistant_unit = "ecampusconnect";
    private EntityManagerFactory emf;
    protected EntityManager em;
    
    protected void openEntityManager() {
	emf = Persistence.createEntityManagerFactory( persistant_unit );    
	em = emf.createEntityManager( );
    }
    
    protected void closeEntityManager() {
	em.close();
	emf.close();
    }
}