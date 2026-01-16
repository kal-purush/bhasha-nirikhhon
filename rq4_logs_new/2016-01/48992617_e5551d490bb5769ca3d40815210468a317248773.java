package grid.DAOImpl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import grid.entities.Project;
import grid.interfaces.DAO.ProjectDAO;


public class ProjectDAOImpl implements ProjectDAO{
	
	private static final Logger logger = LoggerFactory.getLogger(ProjectDAOImpl.class);
	 
    private SessionFactory sessionFactory;
     
    public void setSessionFactory(SessionFactory sf){
        this.sessionFactory = sf;
    }
 
	@SuppressWarnings("unchecked")
	@Override
	public List<Project> listProjects() {
        Session session = this.sessionFactory.getCurrentSession();
        List<Project> projectsList = session.createQuery("from Project").list();
        for(Project p : projectsList){
            logger.info("Project List::"+p);
        }
        return projectsList;
    }

}