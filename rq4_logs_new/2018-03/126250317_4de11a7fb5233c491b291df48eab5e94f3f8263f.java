package br.utfpr.tdd.ex1;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author andreendo
 */
class Aluno {
    
    private List<Double> notasAtividadesPraticas = new ArrayList<>();
    private double notaProjeto, notaRAA = -1;
    int faltas;
    
    public Aluno(String ra, String nome) {
    }

    public void addNotaAtividadePratica(double nota) {
        notasAtividadesPraticas.add(nota);
    }

    public double getNAP() {
        if(notasAtividadesPraticas.isEmpty())
            return 0.0;
        
        double nap = 0; 
        for (Double nota : notasAtividadesPraticas) {
            nap += nota;
        }
        
        return nap / notasAtividadesPraticas.size();
    }

    void setNotaProjeto(double nota) {
        notaProjeto = nota;
    }

    double getNF() {
        if(notaRAA < 0)
            return (notaProjeto + getNAP()) / 2.0;
        
        return (notaProjeto + getNAP() + notaRAA) / 3.0;
    }

    void setNotaRAA(double nota) {
        notaRAA = nota;
    }

    void setFaltas(int faltas) {
        this.faltas = faltas;
    }

    int getFaltas() {
        return faltas;
    }
    
}