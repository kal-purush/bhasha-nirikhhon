package test;

import java.util.List;

import mx.com.nok.equipo.model.business.EquipoBusiness;
import mx.com.nok.equipo.model.dto.DiagnosticoDTO;

public class DiagnosticoTest {

	public static void main (String[] args){
		DiagnosticoDTO dto = new DiagnosticoDTO();
		EquipoBusiness equipoBusiness = new EquipoBusiness();
		List<DiagnosticoDTO> list = equipoBusiness.getDiagnosticos(dto);
		System.out.println(list);
	}
}