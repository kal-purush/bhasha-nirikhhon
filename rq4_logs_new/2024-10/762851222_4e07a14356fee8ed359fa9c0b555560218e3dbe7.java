package school.sptech.backend.service.condominio.view;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "v_total_produtos_arrecadados_por_mes_condominio")
public class ProdutosArrecadadosPorCondominio {

    @Id
    private String nomeCondominio;
    private String mes;
    private Integer count;
}