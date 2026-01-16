package doe.model;

import java.io.Serializable;
import javax.persistence.*;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Date;
import java.util.List;

@Entity
@Table(name="Doacao")
public class Doacao implements Serializable {
	
	@Id
	@GeneratedValue(strategy=GenerationType.AUTO)
	@Column(name="IdDoacao")
	private long idDoacao;

	@Temporal(TemporalType.DATE)
	@Column(name="DataCriacao")
	private Date dataCriacao;
	
	@JsonIgnore
	@OneToMany(mappedBy="doacao")
	private List<DoacaoItem> itens;
	
	@ManyToOne
	@JoinColumn(name="IdUsuario")
	private Usuario usuario;
	
	public Doacao() {
	}

	public long getIdDoacao() {
		return idDoacao;
	}

	public void setIdDoacao(long idDoacao) {
		this.idDoacao = idDoacao;
	}

	public Date getDataCriacao() {
		return dataCriacao;
	}

	public void setDataCriacao(Date dataCriacao) {
		this.dataCriacao = dataCriacao;
	}

	public List<DoacaoItem> getItens() {
		return itens;
	}

	public void setItens(List<DoacaoItem> itens) {
		this.itens = itens;
	}

	public Usuario getUsuario() {
		return usuario;
	}

	public void setUsuario(Usuario usuario) {
		this.usuario = usuario;
	}
	
	
}