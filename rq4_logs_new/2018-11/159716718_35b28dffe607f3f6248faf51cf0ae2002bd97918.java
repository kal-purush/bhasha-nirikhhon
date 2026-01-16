package CodiceFiscale;

import java.util.Calendar;

public final class CodiceFiscale {

    private final String codiceFiscale;
    private final String codiceFiscaleNormalizzato;
    private final String cognome;
    private final String nome;
    private final String annoNascita;
    private final String meseNascita;
    private final String giornoNascita;
    private final String codiceBelfiore;
    private final String codiceBelfioreNormalizzato;
    private final String codiceControllo;
    private final Calendar dataNascita;
    private final String sesso;
    private final boolean omocodice;
    private int livelloOmocodia = 0;

    protected CodiceFiscale(String codiceFiscale, String codiceFiscaleNormalizzato, Calendar dataNascita, String sesso, int livelloOmocodia) {

        this.codiceFiscale = codiceFiscale;
        this.codiceFiscaleNormalizzato = codiceFiscaleNormalizzato;
        this.cognome = this.codiceFiscaleNormalizzato.substring(Constants.INIZIO_COGNOME, Constants.FINE_COGNOME);
        this.nome = this.codiceFiscaleNormalizzato.substring(Constants.INIZIO_NOME, Constants.FINE_NOME);
        this.annoNascita = this.codiceFiscale.substring(Constants.INIZIO_ANNO_NASCITA, Constants.FINE_ANNO_NASCITA);
        this.meseNascita = this.codiceFiscale.substring(Constants.INIZIO_MESE_NASCITA, Constants.FINE_MESE_NASCITA);
        this.giornoNascita = this.codiceFiscale.substring(Constants.INIZIO_GIORNO_NASCITA, Constants.FINE_GIORNO_NASCITA);
        this.codiceBelfiore = this.codiceFiscale.substring(Constants.INIZIO_CODICE_BELFIORE, Constants.FINE_CODICE_BELFIORE);
        this.codiceBelfioreNormalizzato = this.codiceFiscaleNormalizzato.substring(Constants.INIZIO_CODICE_BELFIORE, Constants.FINE_CODICE_BELFIORE);
        this.codiceControllo = this.codiceFiscale.substring(Constants.INIZIO_CODICE_CONTROLLO, Constants.FINE_CODICE_CONTROLLO);

        this.dataNascita = dataNascita;
        this.livelloOmocodia = livelloOmocodia;

        this.omocodice = this.livelloOmocodia > 0;

        this.sesso = sesso;

    }

    public String getCodiceFiscale() {
        return codiceFiscale;
    }

    public String getCodiceFiscaleNormalizzato() {
        return codiceFiscaleNormalizzato;
    }

    public String getCognome() {
        return cognome;
    }

    public String getNome() {
        return nome;
    }

    public String getAnnoNascita() {
        return annoNascita;
    }

    public String getMeseNascita() {
        return meseNascita;
    }

    public String getGiornoNascita() {
        return giornoNascita;
    }

    public Calendar getDataNascita() {
        return dataNascita;
    }

    public String getSesso() {
        return sesso;
    }

    public String getCodiceBelfiore() {
        return codiceBelfiore;
    }

    public String getCodiceControllo() {
        return codiceControllo;
    }

    public boolean isOmocodice() {
        return omocodice;
    }

    public int getLivelloOmocodia() {
        return livelloOmocodia;
    }

    public int getAnnoNascitaNormalizzato() {

        int anno = this.dataNascita.get(Calendar.YEAR);

        return anno;
    }

    public int getMeseNascitaNormalizzato() {

        int mese = this.dataNascita.get(Calendar.MONTH);

        return mese;
    }

    public int getGiornoNascitaNormalizzato() {

        int giorno = this.dataNascita.get(Calendar.DAY_OF_MONTH);

        return giorno;
    }

    public String getCodiceBelfioreNormalizzato() {

        return codiceBelfioreNormalizzato;
    }

    @Override
    public String toString() {
        return "CodiceFiscale{" + "codiceFiscale=" + codiceFiscale + ", codiceFiscaleNormalizzato=" + codiceFiscaleNormalizzato + ", cognome=" + cognome + ", nome=" + nome + ", annoNascita=" + annoNascita + ", meseNascita=" + meseNascita + ", giornoNascita=" + giornoNascita + ", codiceBelfiore=" + codiceBelfiore + ", codiceControllo=" + codiceControllo + ", dataNascita=" + dataNascita.getTime() + ", sesso=" + sesso + ", omocodice=" + omocodice + ", livelloOmocodia=" + livelloOmocodia + '}';
    }

}