package com.github.campaign.test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mock;


public class CampaignRepositoryTest {

	@Mock
	private EntityManager manager;
	protected static EntityManagerFactory emf;

	
	@BeforeClass
    public static void createEntityManagerFactory() {
        emf = Persistence.createEntityManagerFactory("job");
    }

    @AfterClass
    public static void closeEntityManagerFactory() {
        emf.close();
    }
	
	@Before
	public void setUp(){
		manager = emf.createEntityManager();
		manager.getTransaction().begin();
	}
}