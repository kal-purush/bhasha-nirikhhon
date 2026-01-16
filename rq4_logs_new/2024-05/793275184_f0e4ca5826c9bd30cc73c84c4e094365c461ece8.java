package dao;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.Transaction;

import datos.Contacto;

public class ContactoDao {

	private static Session session;
	private Transaction tx;

	private void iniciaOperacion() throws HibernateException {
		session = HibernateUtil.getSessionFactory().openSession();
		tx = session.beginTransaction();
	}

	private void manejaExcepcion(HibernateException he) throws HibernateException {
		tx.rollback();
		throw new HibernateException("ERROR en la capa de acceso: " + he);
	}

	public int agregar(Contacto objeto) {
		int id = 0;
		try {
			iniciaOperacion();
			id = Integer.parseInt(session.save(objeto).toString());
			tx.commit();

		} catch (HibernateException he) {
			manejaExcepcion(he);
			throw he;
		} finally {
			session.close();
		}
		return id;
	}

	public Contacto traer(long idContacto) {
		Contacto objeto = null;
		try {
			iniciaOperacion();
			objeto = (Contacto) session.get(Contacto.class, idContacto);
		} finally {
			session.close();
		}
		return objeto;
	}
	
	public Contacto traer(String email) {
		Contacto objeto = null;
		try {
			iniciaOperacion();
			String hql = "from Contacto c where email=:email";
			objeto = (Contacto) session.createQuery(hql).setParameter("email", email).uniqueResult();
		}finally{
			session.close();
		}
		return objeto;
	}

	public void actualizarObjeto(Contacto objeto) {
		try {
			iniciaOperacion();
			session.update(objeto);
			tx.commit();
		} catch (HibernateException he) {
			manejaExcepcion(he);
			throw he;
		} finally {
			session.close();
		}

	}

	public void eliminar(Contacto objeto) {
		try {
			iniciaOperacion();
			session.delete(objeto);
			tx.commit();
		} catch (HibernateException he) {
			manejaExcepcion(he);
			throw he;
		} finally {
			session.close();
		}
	}
}