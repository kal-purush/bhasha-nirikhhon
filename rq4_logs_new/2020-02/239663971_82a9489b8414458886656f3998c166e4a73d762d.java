package pe.edu.ceid.simi.management.application.estudiante;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import pe.edu.ceid.simi.management.domain.estudiante.model.Estudiante;
import pe.edu.ceid.simi.management.domain.estudiante.model.EstudianteDTO;
import pe.edu.ceid.simi.management.domain.estudiante.repository.EstudianteRepository;

@Service
public class EstudianteServiceImpl implements EstudianteService {

	@Autowired
	private EstudianteRepository repository;
	
	@Override
	public Estudiante crearEstudiante(Estudiante estudiante) {
		// TODO Auto-generated method stub
		return this.repository.crearEstudiante(estudiante);
	}

	@Override
	public Estudiante editEstudiante(Estudiante estudiante, int id) {
		// TODO Auto-generated method stub
		return this.repository.editEstudiante(estudiante, id);
	}

	@Override
	public boolean deleteEstudiante(int id) {
		// TODO Auto-generated method stub
		return this.repository.deleteEstudiante(id);
	}

	@Override
	public List<EstudianteDTO> getEstudiantes() {
		// TODO Auto-generated method stub
		return this.repository.getEstudiantes();
	}

	@Override
	public EstudianteDTO getEstudianteById(int id) {
		// TODO Auto-generated method stub
		return this.repository.getEstudianteById(id);
	}

}