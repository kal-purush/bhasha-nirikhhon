import java.util.ArrayList;

public class GestoreTelefonico {

    private ArrayList<Sim> clienti;
    private ArrayList<Telefonata> telefonate;

    public void creaTelefonata(Sim mittente, Sim destinatario, double durata) {
        Telefonata nuovaTelefonata = new Telefonata(mittente.getNumeroTelefono(), destinatario.getNumeroTelefono(), durata);
        this.telefonate.add(nuovaTelefonata);
        mittente.aggiungiTelefonata(nuovaTelefonata);
        destinatario.aggiungiTelefonata(nuovaTelefonata);
    }

    public GestoreTelefonico(ArrayList<Sim> clienti, ArrayList<Telefonata> telefonate) {
        this.clienti = clienti;
        this.telefonate = telefonate;
    }

    public ArrayList<Sim> getClienti() {
        return clienti;
    }

    public void setClienti(ArrayList<Sim> clienti) {
        this.clienti = clienti;
    }

    public ArrayList<Telefonata> getTelefonate() {
        return telefonate;
    }

    public void setTelefonate(ArrayList<Telefonata> telefonate) {
        this.telefonate = telefonate;
    }
}