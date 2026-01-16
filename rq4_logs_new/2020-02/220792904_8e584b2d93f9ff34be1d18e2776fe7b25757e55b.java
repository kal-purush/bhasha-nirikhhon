package test.com.example.demo.example;

import static org.mockito.ArgumentMatchers.any;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import main.dto.LekarDTO;
import main.dto.TipPregledaDTO;
import main.dto.ZahtevZaPregledDTO;
import main.model.AdministratorKlinike;
import main.model.Lekar;
import main.model.TipPregleda;
import main.model.ZahtevZaPregled;

public class DodajZahtevTests extends PregledServiceTest{

	
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

	@Test
	public void trebaPozvatiLekaraRep_kadaSeMetodaPozove() {
		Mockito.doReturn(getLekar()).when(lekarRepository).findById(Mockito.eq(IDlekar));
		Mockito.doReturn(getTipPregleda()).when(tipPregledaRepository).findById(any(Long.class));

		ZahtevZaPregledDTO zahtevZaPregledDTO = zahtevZaPregledDTO();
		AdministratorKlinike admin = ak();
		service.dodajZahtev(zahtevZaPregledDTO, admin);
		
		Mockito.verify(lekarRepository, Mockito.times(1)).findById(Mockito.eq(IDlekar));
	}
	
	@Test
	public void trebaPozvatiTipPregledaRep_kadaSeMetodaPozove() {
		Mockito.doReturn(getLekar()).when(lekarRepository).findById(any(Long.class));
		Mockito.doReturn(getTipPregleda()).when(tipPregledaRepository).findById(Mockito.eq(IDtipPregleda));
		
		ZahtevZaPregledDTO zahtevZaPregledDTO = zahtevZaPregledDTO();
		
		AdministratorKlinike admin = ak();
		service.dodajZahtev(zahtevZaPregledDTO, admin);
		
		Mockito.verify(tipPregledaRepository, Mockito.times(1)).findById(Mockito.eq(IDtipPregleda));
	}
	
	@Test
	public void trebaSacuvatiZahtev_kadaSeMetodaPozove() {
		Mockito.doReturn(getLekar()).when(lekarRepository).findById(any(Long.class));
		Mockito.doReturn(getTipPregleda()).when(tipPregledaRepository).findById(any(Long.class));
		
		ZahtevZaPregledDTO zahtevZaPregledDTO = zahtevZaPregledDTO();
		
		AdministratorKlinike admin = ak();
		service.dodajZahtev(zahtevZaPregledDTO, admin);
		
		Mockito.verify(zahtevZaPregledRepository, Mockito.times(1)).save(any(ZahtevZaPregled.class));
	}

	@Test
	public void trebaSacuvatiAdmina_kadaSeMetodaPozove() {
		Mockito.doReturn(getLekar()).when(lekarRepository).findById(any(Long.class));
		Mockito.doReturn(getTipPregleda()).when(tipPregledaRepository).findById(any(Long.class));
		
		ZahtevZaPregledDTO zahtevZaPregledDTO = zahtevZaPregledDTO();
		
		AdministratorKlinike admin = ak();
		service.dodajZahtev(zahtevZaPregledDTO, admin);
		
		Mockito.verify(adminKlinikeRepository, Mockito.times(1)).save(any(AdministratorKlinike.class));
	}
	private Optional<Lekar> getLekar() {
		Lekar lekar = new Lekar();
		lekar.setId(IDlekar);
		return Optional.of(lekar);
	}

	
	private Optional<TipPregleda> getTipPregleda() {
		TipPregleda tp = new TipPregleda();
		tp.setId(IDtipPregleda);
		tp.setNaziv("naziv");
		return Optional.of(tp);
	}
	
	private ZahtevZaPregledDTO zahtevZaPregledDTO() {
		ZahtevZaPregledDTO zzpdto = new ZahtevZaPregledDTO();
		LekarDTO lekar = new LekarDTO();
		lekar.setId(IDlekar);
		zzpdto.setLekar(lekar);
		zzpdto.setIdPacijenta(IDpacijent);
		zzpdto.setCena("");
		zzpdto.setDatum("");
		zzpdto.setVreme("");
		zzpdto.setTrajanje(1);
		zzpdto.setVrstaPregleda("");
		TipPregledaDTO tpdto = new TipPregledaDTO();
		tpdto.setId(IDtpdto);
		zzpdto.setTipPregleda(tpdto);
		
		return zzpdto;
		
		}
	
	private AdministratorKlinike ak() {
		AdministratorKlinike ak = new AdministratorKlinike();
		ak.setId(IDadmin);
		ZahtevZaPregled zzp = new ZahtevZaPregled();
		zzp.setId(IDzzp);
		Collection<ZahtevZaPregled> zahtevi = new ArrayList<ZahtevZaPregled>();
		zahtevi.add(zzp);
		ak.setZahtevZaPregled(zahtevi);
		return ak;
	}
		
	}
