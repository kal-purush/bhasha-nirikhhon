package br.senai.sc.dao;

import java.util.List;
import javax.persistence.Query;
import br.senai.sc.entity.Pedido;

public class PedidoDao extends Dao {

	public void salvar(Pedido pedido) {
		getEntityManager().merge(pedido);
	}

	@SuppressWarnings("unchecked")
	public List<Pedido> listarTodos() {
		Query query = getEntityManager().createQuery("From Pedido",Pedido.class);
		return query.getResultList();
	}
	
	public void excluir(Long id) {
		Pedido pedido = getEntityManager().getReference(Pedido.class, id);
		getEntityManager().remove(pedido);
	}

	public Pedido buscarPorId(Long id) {
		return getEntityManager().find(Pedido.class, id);
	}
}