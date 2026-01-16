package main.dto;

import main.model.ZahtevZaPregled;

public class ZahtevZaPregledDTO {
	
	private Long id;
	private String datum;
	private String vreme;
	private SalaDTO sala;
	private TipPregledaDTO tipPregleda;
	private LekarDTO lekar;
	private LekarDTO lekar1;
	private LekarDTO lekar2;
	private Double cena;
	private Long idPacijenta;
	private String trajanje;
	private String vrstaPregleda;
	
	public ZahtevZaPregledDTO() {

	}
	
	public ZahtevZaPregledDTO(ZahtevZaPregled zahtev) {
		this.id = zahtev.getId();
		this.datum = zahtev.getDatum();
		this.vreme = zahtev.getVreme();
		if (zahtev.getSala() != null) {
			this.sala = new SalaDTO(zahtev.getSala());
		}else {
			this.sala = null;
		}
		if (zahtev.getLekar1() != null) {
			this.lekar1 = new LekarDTO(zahtev.getLekar1());
		}else {
			this.lekar1 = null;
		}
		if (zahtev.getLekar2() != null) {
			this.lekar2 = new LekarDTO(zahtev.getLekar2());
		}else {
			this.lekar2 = null;
		}
		this.tipPregleda = new TipPregledaDTO(zahtev.getTipPregleda());
		this.lekar = new LekarDTO(zahtev.getLekar());
		this.cena = zahtev.getCena();
		this.idPacijenta = zahtev.getIdPacijenta();
		this.trajanje = zahtev.getTrajanje();
		this.vrstaPregleda = zahtev.getVrstaPregleda();
	}
	
	public ZahtevZaPregledDTO(Long id, String datum, String vreme, SalaDTO sala, TipPregledaDTO tipPregleda,
			LekarDTO lekar,LekarDTO lekar1, LekarDTO lekar2, Double cena, Long idPacijenta, String trajanje, String vrstaPregleda) {
		super();
		this.id = id;
		this.datum = datum;
		this.vreme = vreme;
		this.sala = sala;
		this.tipPregleda = tipPregleda;
		this.lekar = lekar;
		this.lekar1=lekar1;
		this.lekar2=lekar2;
		this.cena = cena;
		this.idPacijenta = idPacijenta;
		this.trajanje = trajanje;
		this.vrstaPregleda = vrstaPregleda;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getDatum() {
		return datum;
	}

	public void setDatum(String datum) {
		this.datum = datum;
	}

	public String getVreme() {
		return vreme;
	}

	public void setVreme(String vreme) {
		this.vreme = vreme;
	}

	public SalaDTO getSala() {
		return sala;
	}

	public void setSala(SalaDTO sala) {
		this.sala = sala;
	}

	public TipPregledaDTO getTipPregleda() {
		return tipPregleda;
	}

	public void setTipPregleda(TipPregledaDTO tipPregleda) {
		this.tipPregleda = tipPregleda;
	}

	public LekarDTO getLekar() {
		return lekar;
	}

	public void setLekar(LekarDTO lekar) {
		this.lekar = lekar;
	}

	public LekarDTO getLekar1() {
		return lekar1;
	}

	public void setLekar1(LekarDTO lekar1) {
		this.lekar1 = lekar1;
	}

	public LekarDTO getLekar2() {
		return lekar2;
	}

	public void setLekar2(LekarDTO lekar2) {
		this.lekar2 = lekar2;
	}

	public Double getCena() {
		return cena;
	}

	public void setCena(Double cena) {
		this.cena = cena;
	}

	public Long getIdPacijenta() {
		return idPacijenta;
	}

	public void setIdPacijenta(Long idPacijenta) {
		this.idPacijenta = idPacijenta;
	}

	public String getTrajanje() {
		return trajanje;
	}

	public void setTrajanje(String trajanje) {
		this.trajanje = trajanje;
	}

	public String getVrstaPregleda() {
		return vrstaPregleda;
	}

	public void setVrstaPregleda(String vrstaPregleda) {
		this.vrstaPregleda = vrstaPregleda;
	}
	
	

}