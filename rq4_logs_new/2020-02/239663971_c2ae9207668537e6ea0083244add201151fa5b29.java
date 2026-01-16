package pe.edu.ceid.simi.management.application.aula;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import pe.edu.ceid.simi.management.domain.aula.model.Aula;
import pe.edu.ceid.simi.management.domain.aula.model.AulaDTO;
import pe.edu.ceid.simi.management.domain.aula.repository.AulaRepository;


@Service
public class AulaServiceImpl implements AulaService {

	@Autowired
	private AulaRepository repository;
	
	@Override
	public Aula crearAula(Aula aula) {
		// TODO Auto-generated method stub
		return this.repository.crearAula(aula);
	}

	@Override
	public Aula editAula(Aula aula, int id) {
		// TODO Auto-generated method stub
		return this.repository.editAula(aula, id);
	}

	@Override
	public List<AulaDTO> getAulas() {
		// TODO Auto-generated method stub
		return this.repository.getAulas();
	}

	@Override
	public AulaDTO getAulaById(int id) {
		// TODO Auto-generated method stub
		return this.repository.getAulaById(id);
	}

	@Override
	public boolean deleteAula(int id) {
		// TODO Auto-generated method stub
		return this.repository.deleteAula(id);
	}

}