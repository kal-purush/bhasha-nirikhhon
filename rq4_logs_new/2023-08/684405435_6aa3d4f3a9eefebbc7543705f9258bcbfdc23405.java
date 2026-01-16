class Bus {
    private String patente;
    private String nombreChofer;
    private String rutChofer;

    public Bus(String patente, String nombreChofer, String rutChofer) {
        this.patente = patente;
        this.nombreChofer = nombreChofer;
        this.rutChofer = rutChofer;
    }

    // Getters y setters
    public String getPatente() {
        return patente;
    }

    public void setPatente(String patente) {
        this.patente = patente;
    }

    public String getNombreChofer() {
        return nombreChofer;
    }

    public void setNombreChofer(String nombreChofer) {
        this.nombreChofer = nombreChofer;
    }

    public String getRutChofer() {
        return rutChofer;
    }

    public void setRutChofer(String rutChofer) {
        this.rutChofer = rutChofer;
    }
}