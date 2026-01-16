package pe.edu.ceid.simi.management.application.docenteusuario;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import pe.edu.ceid.simi.management.domain.docenteusuario.model.DocenteUsuario;
import pe.edu.ceid.simi.management.domain.docenteusuario.repository.DocenteUsuarioRepository;

public class DocenteUsuarioServiceImpl implements DocenteUsuarioService {

	@Autowired
	private DocenteUsuarioRepository repository;
	
	@Override
	public DocenteUsuario crearDocenteUsuario(DocenteUsuario docenteUsuario) {
		// TODO Auto-generated method stub
		return this.repository.crearDocenteUsuario(docenteUsuario);
	}

	@Override
	public DocenteUsuario editDocenteUsuario(DocenteUsuario docenteUsuario, int id) {
		// TODO Auto-generated method stub
		return this.repository.editDocenteUsuario(docenteUsuario, id);
	}

	@Override
	public List<DocenteUsuario> getDocenteUsuario() {
		// TODO Auto-generated method stub
		return this.repository.getDocenteUsuario();
	}

	@Override
	public boolean deleteDocenteUsuario(int cdocenteUsuario) {
		// TODO Auto-generated method stub
		return this.repository.deleteDocenteUsuario(cdocenteUsuario);
	}

	@Override
	public DocenteUsuario getDocenteUsuarioById(int id) {
		// TODO Auto-generated method stub
		return this.repository.getDocenteUsuarioById(id);
	}

}