package tablerocolores.cfic.edu.tugramola.dto;


public class PersonaFila {

    private int id;
    private String nombre;
    private String foto;
    private String git;
    private String lin;
    private String gitIco;
    private String linIco;


    public PersonaFila() {}

    public PersonaFila(int id, String nombre, String foto, String lin, String git) {
        this.id = id;
        this.nombre = nombre;
        this.foto = foto;
        this.git = git;
        this.lin = lin;
    }


    public int getId() {
        return id;
    }

    public String getNombre() {
        return nombre;
    }

    public String getFoto() {
        return foto;
    }

    public String getGit() {
        return git;
    }

    public String getLin() {
        return lin;
    }

    public String getGitIco() {
        return gitIco;
    }

    public String getLinIco() {
        return linIco;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public void setFoto(String foto) {
        this.foto = foto;
    }

    public void setGit(String git) {
        this.git = git;
    }

    public void setLin(String lin) {
        this.lin = lin;
    }

    public void setGitIco(String gitIco) {
        this.gitIco = gitIco;
    }

    public void setLinIco(String linIco) {
        this.linIco = linIco;
    }
}