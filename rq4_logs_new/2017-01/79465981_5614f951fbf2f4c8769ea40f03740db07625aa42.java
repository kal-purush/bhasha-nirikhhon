package webrefeicoes.dao;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.Transaction;

import webrefeicoes.model.Funcionario;

public class FuncionarioDAO implements FuncionarioDAOInterface{

	@Override
	public void save(Funcionario funcionario) {
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction t = session.beginTransaction();
		session.save(funcionario);
		t.commit();
	}

	@Override
	public Funcionario getFuncionario(long id) {
		Session session = HibernateUtil.getSessionFactory().openSession();
		return (Funcionario) session.load(Funcionario.class, id);
	}

	@Override
	public List<Funcionario> list() {
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction t = session.beginTransaction();
		List<Funcionario> lista = session.createQuery("from funcionario").list();
		t.commit();
		return lista;
	}

	@Override
	public void remove(Funcionario funcionario) {
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction t = session.beginTransaction();
		session.delete(funcionario);
		t.commit();
	}

	@Override
	public void update(Funcionario funcionario) {
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction t = session.beginTransaction();
		session.update(funcionario);
		t.commit();
		
	}

}