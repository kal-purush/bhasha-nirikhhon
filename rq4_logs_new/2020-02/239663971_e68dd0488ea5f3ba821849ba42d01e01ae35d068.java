package management.infraestructure.alumno.jdbc;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import management.domain.alumno.model.Alumno;
import management.domain.alumno.repository.AlumnoRepository;

@Component
public class AlumnoRepositoryImpl implements AlumnoRepository{

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private AlumnoRowMapper row;
	
	@Override
	public List<Alumno> getAlumno() {
		String findAlumnos = "select * from tmAlumnos";
		List<Alumno> alumnos = this.row.getAlumnos(this.jdbcTemplate.queryForList(findAlumnos));
		return alumnos;
	}

}