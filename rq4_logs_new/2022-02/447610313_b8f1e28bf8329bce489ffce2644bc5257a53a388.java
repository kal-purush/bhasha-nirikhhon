package dad.virus.jasper;

import java.time.LocalDateTime;
import java.util.Date;

public class Resultado{
    private long tiempoJugado;
    private Date fechaPartida;

    public Resultado(long tiempoJugado, Date fechaPartida) {
        this.tiempoJugado = tiempoJugado;
        this.fechaPartida = fechaPartida;
    }
}