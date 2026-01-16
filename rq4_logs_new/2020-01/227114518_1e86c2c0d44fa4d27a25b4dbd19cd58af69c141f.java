package no.nav.foreldrepenger.autotest.dokumentgenerator.foreldrepengesoknad.builders;

import no.nav.foreldrepenger.autotest.dokumentgenerator.foreldrepengesoknad.SøkersRolle;
import no.nav.vedtak.felles.xml.soeknad.felles.v3.*;
import no.nav.vedtak.felles.xml.soeknad.foreldrepenger.v3.Dekningsgrad;
import no.nav.vedtak.felles.xml.soeknad.foreldrepenger.v3.Foreldrepenger;
import no.nav.vedtak.felles.xml.soeknad.foreldrepenger.v3.ObjectFactory;
import no.nav.vedtak.felles.xml.soeknad.foreldrepenger.v3.Opptjening;
import no.nav.vedtak.felles.xml.soeknad.kodeverk.v3.Dekningsgrader;
import no.nav.vedtak.felles.xml.soeknad.uttak.v3.Fordeling;
import no.nav.vedtak.felles.xml.soeknad.v3.Soeknad;

public class ForeldrepengerBuilder extends SøknadBuilder<ForeldrepengerBuilder> {

    private Foreldrepenger foreldrepengerKladd;

    public ForeldrepengerBuilder(String aktoerId, SøkersRolle søkerRolle) {
        foreldrepengerKladd = new Foreldrepenger();
        medSøker(aktoerId, søkerRolle);
    }
    @Override
    protected ForeldrepengerBuilder self(){
        return this;
    }
    @Override
    protected ForeldrepengerBuilder medYtelse(Ytelse ytelse) {
        søknadKladd.setOmYtelse(setOmYtelseJAXBElement(
                (new ObjectFactory()).createForeldrepenger((Foreldrepenger) ytelse)));
        return this;
    }

    public ForeldrepengerBuilder medFordeling(Fordeling fordeling) {
        foreldrepengerKladd.setFordeling(fordeling);
        return this;
    }
    public ForeldrepengerBuilder medRelasjonTilBarnet(SoekersRelasjonTilBarnet soekersRelasjonTilBarnet) {
        foreldrepengerKladd.setRelasjonTilBarnet(soekersRelasjonTilBarnet);
        return this;
    }
    public ForeldrepengerBuilder medDekningsgrad(String dekningsgradString){
        Dekningsgrad dekningsgrad = new Dekningsgrad();
        Dekningsgrader dekningsgrader = new Dekningsgrader();
        dekningsgrader.setKode(dekningsgradString);
        dekningsgrader.setKodeverk("IKKE_DEFINERT");
        dekningsgrad.setDekningsgrad(dekningsgrader);

        foreldrepengerKladd.setDekningsgrad(dekningsgrad);
        return this;
    }
    public ForeldrepengerBuilder medMedlemskap(Medlemskap medlemskap) {
        foreldrepengerKladd.setMedlemskap(medlemskap);
        return this;
    }
    public ForeldrepengerBuilder medRettigheter(Rettigheter rettigheter) {
        foreldrepengerKladd.setRettigheter(rettigheter);
        return this;
    }
    public ForeldrepengerBuilder medAnnenForelder(AnnenForelder annenForelder) {
        foreldrepengerKladd.setAnnenForelder(annenForelder);
        return this;
    }
    public ForeldrepengerBuilder medAnnenForelder(String annenForelderAktørId) {
        foreldrepengerKladd.setAnnenForelder(standardAnnenForelder(annenForelderAktørId));
        return this;
    }
    public ForeldrepengerBuilder medSpesiellOpptjening(Opptjening opptjening) {
        foreldrepengerKladd.setOpptjening(opptjening);
        return this;
    }


    public Soeknad build() {
        medYtelse(foreldrepengerKladd);
        return super.build();
    }


}