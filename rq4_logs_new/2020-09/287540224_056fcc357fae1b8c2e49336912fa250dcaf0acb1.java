package no.nav.service.pensjon.regler.to;

import no.nav.domain.pensjon.regler.krav.Kravhode;
import no.stelvio.common.transferobject.ServiceRequest;

import java.util.Date;

public class VilkarsprovForsorgingstilleggRequest extends ServiceRequest {
    private Kravhode kravhode;
    private Date virkFom;
    private Date virkTom;

    public VilkarsprovForsorgingstilleggRequest() {
        super();
    }

    public VilkarsprovForsorgingstilleggRequest(Kravhode kravhode, Date virkFom, Date virkTom) {
        super();
        this.kravhode = kravhode;
        this.virkFom = virkFom;
        this.virkTom = virkTom;
    }

    public Kravhode getKravhode() {
        return kravhode;
    }

    public void setKravhode(Kravhode kravhode) {
        this.kravhode = kravhode;
    }

    public Date getVirkFom() {
        return virkFom;
    }

    public void setVirkFom(Date virkFom) {
        this.virkFom = virkFom;
    }

    public Date getVirkTom() {
        return virkTom;
    }

    public void setVirkTom(Date virkTom) {
        this.virkTom = virkTom;
    }
}