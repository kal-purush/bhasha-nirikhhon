package pe.edu.ceid.simi.operative.application.alumno;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import pe.edu.ceid.simi.operative.domain.alumno.model.Alumno;
import pe.edu.ceid.simi.operative.domain.alumno.model.AlumnoDTO;
import pe.edu.ceid.simi.operative.domain.alumno.repository.AlumnoRepository;

public class AlumnoServiceImpl implements AlumnoService{

	private AlumnoRepository repository;
	
	@Override
	public AlumnoDTO getAlumnoById(int id) {
		// TODO Auto-generated method stub
		return this.repository.getAlumnoById(id);
	}

}