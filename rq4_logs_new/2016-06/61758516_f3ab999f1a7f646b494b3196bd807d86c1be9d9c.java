import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Calendar; 

public class Agenda {
	
    private List<Turma> turmas = new LinkedList<Turma>();
	
	private int nrTurmas;
	
	private List<Usuario> usuarios = new LinkedList<Usuario>();
	
	private int nrUsuarios;
	
	private List<Equipe> equipes = new LinkedList<Equipe>();

	//Map<String, ContaCorrente> mapaDeContas = new HashMap<>();
	
	public List<Equipe> getEquipes() {
		return equipes;
	}

	public void setEquipes(Equipe equipe) {
		this.equipes.add(equipe);
	}

	public List<Usuario> getUsuarios() {
		return usuarios;
	}

	public void setUsuarios(Usuario us) {
		this.usuarios.add(us);
	}

	public int getNrUsuarios() {
		return nrUsuarios;
	}

	public void setNrUsuarios(int nrUsuarios) {
		this.nrUsuarios = nrUsuarios;
	}

	public int getNrTurmas() {
		return nrTurmas;
	}

	public void setNrTurmas(int nrTurmas) {
		this.nrTurmas = nrTurmas;
	}
	
}