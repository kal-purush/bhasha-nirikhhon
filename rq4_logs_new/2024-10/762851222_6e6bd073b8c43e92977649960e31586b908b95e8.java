package school.sptech.backend.domain.produtounitario.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import school.sptech.backend.service.produtounitario.view.QtdProdutoPorCampanha;
import school.sptech.backend.service.produtounitario.view.QtdProdutosVencidosPorCampanha;

import java.util.List;

public interface QtdProdutosVencidosPorCampanhaRepository extends JpaRepository<QtdProdutosVencidosPorCampanha, Integer> {
    List<QtdProdutosVencidosPorCampanha> findByProdutoId(Integer id);
}