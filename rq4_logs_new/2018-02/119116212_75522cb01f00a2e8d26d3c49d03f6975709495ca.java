package br.com.mars.sonda.models;

import javax.validation.constraints.Size;
import java.util.ArrayList;
import java.util.List;

public class ComandosViewModel {

    @Size(min = 1)
    private List<Comando> comandos = new ArrayList<>();

    public List<Comando> getComandos() {
        return comandos;
    }

    public void setComandos(List<Comando> comandos) {
        this.comandos = comandos;
    }
}