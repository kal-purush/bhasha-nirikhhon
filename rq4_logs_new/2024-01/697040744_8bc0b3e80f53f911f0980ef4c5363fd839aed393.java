package br.com.fiap.soat.grupo48.application.pedido.valueobject;

import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.WeekFields;

public class GeradorDeNumeroSequencial {
    private static GeradorDeNumeroSequencial instance;

    private int numeroSequencial = 0;
    private LocalDate ultimoReset = LocalDate.MIN;
    private String letraMes;
    private String letraSemana;

    private DecimalFormat df;
    private GeradorDeNumeroSequencial() {
        df = new DecimalFormat("000000");
    }

    public static GeradorDeNumeroSequencial getInstance() {
        if (instance == null) {
            synchronized (GeradorDeNumeroSequencial.class) {
                if (instance == null) {
                    instance = new GeradorDeNumeroSequencial();
                }
            }
        }
        return instance;
    }

    public String proximoNumero() {
        LocalDate hoje = LocalDate.now();
        if (!ultimoReset.equals(hoje)) {
            numeroSequencial = 0;
            ultimoReset = hoje;
            var dataHoraAtual = LocalDateTime.now();
            var mes = dataHoraAtual.getMonthValue();
            var semana = dataHoraAtual.get(WeekFields.ISO.weekOfWeekBasedYear());

            letraMes = Character.toString((char)(mes + 'A' - 1));
            letraSemana = Character.toString((char)(semana + 'A' - 1));
        }
        var proximoNumero = letraMes + letraSemana + df.format(++numeroSequencial);
        return proximoNumero;
    }
}