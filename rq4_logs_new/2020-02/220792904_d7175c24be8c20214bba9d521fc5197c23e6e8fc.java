package test.com.example.demo.example;

import static org.mockito.ArgumentMatchers.any;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;

import main.dto.KlinikaDTO;
import main.dto.LekarDTO;
import main.dto.PregledDTO;
import main.dto.SalaDTO;
import main.dto.TipPregledaDTO;
import main.model.Klinika;
import main.model.Lekar;
import main.model.Pregled;
import main.model.Sala;
import main.model.TipPregleda;

public class DodajPregledTests extends PregledServiceTest{
	
	@Test
	public void trebaPozvatiLekaraRep_kadaSeMetodaPozove() {
		Mockito.doReturn(getLekar()).when(lekarRepository).findById(any(Long.class));
		Mockito.doReturn(getSala()).when(salaKlinikeRepository).findById(any(Long.class));
		Mockito.doReturn(getTipPregleda()).when(tipPregledaRepository).findById(any(Long.class));
		
		PregledDTO pregledDTO = pregledDTO();
		
		service.dodajPregled(pregledDTO);
		
		Mockito.verify(lekarRepository, Mockito.times(1)).findById(any(Long.class));
	}
	/*
	@Test
	public void trebaPozvatiSalaRep_kadaSeMetodaPozove() {
		Mockito.doReturn(getSala()).when(salaKlinikeRepository.findById(any(Long.class)));
	}
	
	@Test
	public void trebaPozvatiTipPregledaRep_kadaSeMetodaPozove() {
		Mockito.doReturn(getTipPregleda()).when(tipPregledaRepository.findById(any(Long.class)));
	}*/
	
	
	private Optional<Lekar> getLekar() {
		Lekar lekar = new Lekar();
		lekar.setAdresa("Radnicka");
		lekar.setDrzava("Srbija");
		lekar.setGrad("NS");
		lekar.setIme("Ana");
		lekar.setPrezime("Nik");
		lekar.setEmail("bla@gmail.com");
		lekar.setJmbg("123");
		lekar.setId((long) 1);
		Klinika klinika = new Klinika();
		klinika.setId((long)1);
		lekar.setKlinika(klinika);
		lekar.setBrojRecenzija(0);
		return Optional.of(lekar);
	}
	
	private Optional<Sala> getSala() {
		Sala sala = new Sala();
		sala.setId((long) 1);
		Pregled pregled = new Pregled();
		pregled.setId((long) 1);
		Collection<Pregled> pregledi = new ArrayList<Pregled>();
		pregledi.add(pregled);
		sala.setPregledi(pregledi);
		return Optional.of(sala);
	}
	
	private Optional<TipPregleda> getTipPregleda() {
		TipPregleda tp = new TipPregleda();
		tp.setId((long)1);
		tp.setNaziv("naziv");
		return Optional.of(tp);
	}
	
	private PregledDTO pregledDTO() {
		PregledDTO pregleddto = new PregledDTO();
		pregleddto.setId((long)1);
		pregleddto.setCena("");
		pregleddto.setDatum("");
		pregleddto.setVreme("");
		TipPregledaDTO tpdto = new TipPregledaDTO();
		tpdto.setId((long)1);
		pregleddto.setTipPregleda(tpdto);
		pregleddto.setIdPacijenta((long)1);
		pregleddto.setTrajanjePregleda(1);
		LekarDTO lekar = new LekarDTO();
		lekar.setId((long)1);
		pregleddto.setLekar(lekar);
		SalaDTO saladto = new SalaDTO();
		saladto.setId((long)1);
		pregleddto.setSala(saladto);
		return pregleddto;
	}
}