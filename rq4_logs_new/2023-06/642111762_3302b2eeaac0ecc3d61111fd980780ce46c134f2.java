package mx.com.gm.Dao;

import mx.com.gm.Models.Usuario;
import java.util.List;

import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;


@Repository
@Transactional
//esta clase lo que hace es implementar la Interface usuarioDao 
public class usuarioDaoImp implements usuarioDao {

	@PersistenceContext
	EntityManager entityManager;// sirve para hacer la conexion con la base de datos

	@Override
	public List<Usuario> getUsuarios() {

		String query = "FROM Usuario";
		return entityManager.createQuery(query).getResultList();

	}

	@Override
	public void eliminar(Long id) {
		Usuario usuario = entityManager.find(Usuario.class, id);
		entityManager.remove(usuario);

	}

}