package modelo;

public class VinculoSerieAtor {
    private int idSerie;
    private int idAtor;

    public VinculoSerieAtor() {
        this(-1, -1);
    }

    public VinculoSerieAtor(int idSerie, int idAtor) {
        this.idSerie = idSerie;
        this.idAtor = idAtor;
    }

    public int getIdSerie() {
        return idSerie;
    }

    public void setIdSerie(int idSerie) {
        this.idSerie = idSerie;
    }

    public int getIdAtor() {
        return idAtor;
    }

    public void setIdAtor(int idAtor) {
        this.idAtor = idAtor;
    }
}
