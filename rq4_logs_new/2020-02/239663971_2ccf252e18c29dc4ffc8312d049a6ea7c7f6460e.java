package simi.application.curso;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import simi.domain.curso.model.Curso;
import simi.domain.curso.model.CursoDTO;
import simi.domain.curso.repository.CursoRepository;

@Service
public class CursoServiceImpl implements CursoService{
	
	@Autowired
	private CursoRepository repository;
	
	
	@Override
	public Curso crearCurso(Curso curso) {
		// TODO Auto-generated method stub
		return this.repository.crearCurso(curso);
	}

	@Override
	public Curso editCurso(Curso curso, int id) {
		// TODO Auto-generated method stub
		return this.repository.editCurso(curso, id);
	}

	@Override
	public List<CursoDTO> getCursos() {
		// TODO Auto-generated method stub
		return this.repository.getCursos();
	}

	@Override
	public CursoDTO getCursoById(int id) {
		// TODO Auto-generated method stub
		return this.repository.getCursoById(id);
	}

	@Override
	public boolean deleteCurso(int id) {
		// TODO Auto-generated method stub
		return this.repository.deleteCurso(id);
	}

}