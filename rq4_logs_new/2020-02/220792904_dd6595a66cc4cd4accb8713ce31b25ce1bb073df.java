package test.com.example.demo.example;

import static org.mockito.ArgumentMatchers.any;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import main.dto.LekarDTO;
import main.dto.PregledDTO;
import main.dto.SalaDTO;
import main.dto.TipPregledaDTO;
import main.dto.ZahtevZaPregledDTO;
import main.model.Klinika;
import main.model.Lekar;
import main.model.Pregled;
import main.model.Sala;
import main.model.TipPregleda;
import main.model.ZahtevZaPregled;

public class OdobriTests extends ZahtevZaPregledServiceTest{

	
	Long IDlekar = (long) 1;
	Long IDsala = (long) 1;
	Long IDtipPregleda = (long) 1;
	Long IDpregled = (long) 1;
	Long IDpacijentdto = (long) 1;
	Long IDsaladto = (long) 1;
	Long IDpregleddto = (long) 1;
	Long IDlekardto = (long) 1;
	Long IDklinikadto = (long) 1;
	Long IDpacijent = (long) 1;
	Long IDtpdto = (long) 1;
	Long IDadmin = (long) 1;
	Long IDzzp = (long) 1;
	
	public void trebaSacuvatiPregled_kadaSeMetodaPozove() {
		Mockito.doReturn(getLekar()).when(lekarRepository).findById(any(Long.class));
		Mockito.doReturn(getSala()).when(salaKlinikeRepository).getOne(any(Long.class));
		Mockito.doReturn(getTipPregleda()).when(tipPregledaRepository).getOne(any(Long.class));
		Mockito.doReturn(getSale()).when(salaKlinikeRepository).findAll();
		Mockito.doReturn(getZahtevi()).when(zahtevZaPregledRepository).findAll();

		ZahtevZaPregledDTO zahtevZaPregledDTO = getZZPDTO();
		
		service.odobri(zahtevZaPregledDTO);
		
		Mockito.verify(pregledRepository, Mockito.times(1)).save(any(Pregled.class));
	}
	
	@Test
	public void trebaPozvatiLekaraRep_kadaSeMetodaPozove() {
		Mockito.doReturn(getLekar()).when(lekarRepository).findById(any(Long.class));
		Mockito.doReturn(getSala()).when(salaKlinikeRepository).getOne(any(Long.class));
		Mockito.doReturn(getTipPregleda()).when(tipPregledaRepository).getOne(any(Long.class));
		Mockito.doReturn(getSale()).when(salaKlinikeRepository).findAll();
		Mockito.doReturn(getZahtevi()).when(zahtevZaPregledRepository).findAll();

		ZahtevZaPregledDTO zahtevZaPregledDTO = getZZPDTO();
		
		service.odobri(zahtevZaPregledDTO);
		
		Mockito.verify(lekarRepository, Mockito.times(1)).findById(any(Long.class));
	}
	
	@Test
	public void trebaPozvatiSalaRep_kadaSeMetodaPozove() {
		Mockito.doReturn(getLekar()).when(lekarRepository).findById(any(Long.class));
		Mockito.doReturn(getSala()).when(salaKlinikeRepository).getOne(any(Long.class));
		Mockito.doReturn(getTipPregleda()).when(tipPregledaRepository).getOne(any(Long.class));
		Mockito.doReturn(getSale()).when(salaKlinikeRepository).findAll();
		Mockito.doReturn(getZahtevi()).when(zahtevZaPregledRepository).findAll();

		ZahtevZaPregledDTO zahtevZaPregledDTO = getZZPDTO();
		
		service.odobri(zahtevZaPregledDTO);
		
		Mockito.verify(salaKlinikeRepository, Mockito.times(1)).getOne(any(Long.class));
	}
	
	@Test
	public void trebaPozvatiTipPregledaRep_kadaSeMetodaPozove() {
		Mockito.doReturn(getLekar()).when(lekarRepository).findById(any(Long.class));
		Mockito.doReturn(getSala()).when(salaKlinikeRepository).getOne(any(Long.class));
		Mockito.doReturn(getTipPregleda()).when(tipPregledaRepository).getOne(any(Long.class));
		Mockito.doReturn(getSale()).when(salaKlinikeRepository).findAll();
		Mockito.doReturn(getZahtevi()).when(zahtevZaPregledRepository).findAll();

		ZahtevZaPregledDTO zahtevZaPregledDTO = getZZPDTO();
		
		service.odobri(zahtevZaPregledDTO);
		
		Mockito.verify(tipPregledaRepository, Mockito.times(1)).getOne(any(Long.class));
	}
	
	@Test
	public void trebaPozvatiSveSaleRep_kadaSeMetodaPozove() {
		Mockito.doReturn(getLekar()).when(lekarRepository).findById(any(Long.class));
		Mockito.doReturn(getSala()).when(salaKlinikeRepository).getOne(any(Long.class));
		Mockito.doReturn(getTipPregleda()).when(tipPregledaRepository).getOne(any(Long.class));
		Mockito.doReturn(getSale()).when(salaKlinikeRepository).findAll();
		Mockito.doReturn(getZahtevi()).when(zahtevZaPregledRepository).findAll();

		ZahtevZaPregledDTO zahtevZaPregledDTO = getZZPDTO();
		
		service.odobri(zahtevZaPregledDTO);
		Mockito.verify(salaKlinikeRepository, Mockito.times(1)).findAll();

	}
	
	@Test
	public void trebaPozvatiSveZahteveRep_kadaSeMetodaPozove() {
		Mockito.doReturn(getLekar()).when(lekarRepository).findById(any(Long.class));
		Mockito.doReturn(getSala()).when(salaKlinikeRepository).getOne(any(Long.class));
		Mockito.doReturn(getTipPregleda()).when(tipPregledaRepository).getOne(any(Long.class));
		Mockito.doReturn(getSale()).when(salaKlinikeRepository).findAll();
		Mockito.doReturn(getZahtevi()).when(zahtevZaPregledRepository).findAll();

		ZahtevZaPregledDTO zahtevZaPregledDTO = getZZPDTO();
		
		service.odobri(zahtevZaPregledDTO);
		
		Mockito.verify(zahtevZaPregledRepository, Mockito.times(0)).findAll();
	}

	
	@Test
	public void daLiJeDobarArgLekaraRep_kadaSeMetodaPozove() {
		Mockito.doReturn(getLekar()).when(lekarRepository).findById(IDlekar);
		Mockito.doReturn(getSala()).when(salaKlinikeRepository).getOne(any(Long.class));
		Mockito.doReturn(getTipPregleda()).when(tipPregledaRepository).getOne(any(Long.class));
		Mockito.doReturn(getSale()).when(salaKlinikeRepository).findAll();
		Mockito.doReturn(getZahtevi()).when(zahtevZaPregledRepository).findAll();

		ZahtevZaPregledDTO zahtevZaPregledDTO = getZZPDTO();
		
		service.odobri(zahtevZaPregledDTO);
		
		Mockito.verify(lekarRepository, Mockito.times(1)).findById(IDlekar);
	}
	
	@Test
	public void daLiJeDobarArgSalaRep_kadaSeMetodaPozove() {
		Mockito.doReturn(getLekar()).when(lekarRepository).findById(any(Long.class));
		Mockito.doReturn(getSala()).when(salaKlinikeRepository).getOne(IDsala);
		Mockito.doReturn(getTipPregleda()).when(tipPregledaRepository).getOne(any(Long.class));
		Mockito.doReturn(getSale()).when(salaKlinikeRepository).findAll();
		Mockito.doReturn(getZahtevi()).when(zahtevZaPregledRepository).findAll();

		ZahtevZaPregledDTO zahtevZaPregledDTO = getZZPDTO();
		
		service.odobri(zahtevZaPregledDTO);

		Mockito.verify(salaKlinikeRepository, Mockito.times(1)).getOne(IDsala);
	}
	
	@Test
	public void daLiJeDobarArgTipPregledaRep_kadaSeMetodaPozove() {
		Mockito.doReturn(getLekar()).when(lekarRepository).findById(any(Long.class));
		Mockito.doReturn(getSala()).when(salaKlinikeRepository).getOne(any(Long.class));
		Mockito.doReturn(getTipPregleda()).when(tipPregledaRepository).getOne(IDtipPregleda);
		Mockito.doReturn(getSale()).when(salaKlinikeRepository).findAll();
		Mockito.doReturn(getZahtevi()).when(zahtevZaPregledRepository).findAll();

		ZahtevZaPregledDTO zahtevZaPregledDTO = getZZPDTO();
		
		service.odobri(zahtevZaPregledDTO);
		
		Mockito.verify(tipPregledaRepository, Mockito.times(1)).getOne(IDtipPregleda);
	}
	

	
	
	private Optional<Lekar> getLekar() {
		Lekar lekar = new Lekar();
		lekar.setId(IDlekar);
		return Optional.of(lekar);
	}
	
	private Sala getSala() {
		Sala sala = new Sala();
		sala.setId(IDsala);
		Pregled pregled = new Pregled();
		pregled.setId(IDpregled);
		Collection<Pregled> pregledi = new ArrayList<Pregled>();
		pregledi.add(pregled);
		sala.setPregledi(pregledi);
		return sala;
	}
	
	private TipPregleda getTipPregleda() {
		TipPregleda tp = new TipPregleda();
		tp.setId(IDtipPregleda);
		tp.setNaziv("naziv");
		return tp;
	}
	
	private Klinika getKlinika() {
		Klinika klinika = new Klinika();
		klinika.setId(IDklinikadto);
		return klinika;
	}
	
	private LekarDTO getLekarDTO() {
		LekarDTO lekar = new LekarDTO();
		lekar.setId(IDlekar);
		return lekar;
	}
	
	private SalaDTO getSalaDTO() {
		SalaDTO sala = new SalaDTO();
		sala.setId(IDsala);
		return sala;
	}
	
	private TipPregledaDTO getTipPregledaDTO() {
		TipPregledaDTO tp = new TipPregledaDTO();
		tp.setId(IDtipPregleda);
		tp.setNaziv("naziv");
		return tp;
	}

	private PregledDTO getPregledDTO() {
		PregledDTO pregled = new PregledDTO();
		pregled.setId(IDpregled);
		return pregled;
	}
	
	private Pregled getPregled() {
		Pregled pregled = new Pregled();
		pregled.setId(IDpregled);
		return pregled;
	}
	
	private ZahtevZaPregled getZahtevZaPregled() {
		ZahtevZaPregled pregled = new ZahtevZaPregled();
		pregled.setId(IDpregled);
		return pregled;
	}
	
	private ZahtevZaPregledDTO getZZPDTO() {
		ZahtevZaPregledDTO zzpdto = new ZahtevZaPregledDTO();
		zzpdto.setId(IDpregleddto);
		zzpdto.setDatum("");
		zzpdto.setIdPacijenta(IDpacijent);
		zzpdto.setLekar(getLekarDTO());
		zzpdto.setSala(getSalaDTO());
		zzpdto.setTipPregleda(getTipPregledaDTO());
		zzpdto.setTrajanje(1);
		zzpdto.setVreme("");
		zzpdto.setVrstaPregleda("");
		return zzpdto;
	}
	
	private List<Sala> getSale() {
		ArrayList<Sala> result = new ArrayList<Sala>();
		
		result.add(getSala());
		
		return result;
	}
	
	
	private List<ZahtevZaPregled> getZahtevi() {
		ArrayList<ZahtevZaPregled> result = new ArrayList<ZahtevZaPregled>();
		
		result.add(getZahtevZaPregled());
		
		return result;
	}
}