import org.hibernate.HibernateException;

import hibernate.dao.BDDUtils;
import play.Application;
import play.GlobalSettings;
import play.Logger;

public class Global extends GlobalSettings {

	public void onStart(Application app) {
		Logger.info("Application has started.");
		try {
			BDDUtils.getCurrentSession();
		} catch (HibernateException he) {
			Logger.error("Impossible de se connecter à la base de donnée.");
			Logger.error(BDDUtils.createDBProperties().toString());
		}
	}
	
	public void onStop(Application app) {
		Logger.info("Application shutdown ...");
		try {
			BDDUtils.getCurrentSession().close();
		} catch (HibernateException he) {
			Logger.error("Impossible de se connecter à la base de donnée.");
		}
	}
}