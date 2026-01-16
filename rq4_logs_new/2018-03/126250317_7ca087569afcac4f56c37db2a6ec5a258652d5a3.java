package br.utfpr.tdd.ex1;

import java.util.List;

/**
 *
 * @author andreendo
 */
public class ProcessadorDeNotas {
    private EscritorCSV escritor;
    private LeitorCSV leitor;
    private Avaliador avaliador;
    
    public ProcessadorDeNotas() {
        leitor = new LeitorCSV();
        escritor = new EscritorCSV();
        avaliador = new Avaliador();
    }
    
    public void setEscritor(EscritorCSV escritor) {
        this.escritor = escritor;
    }

    public void setLeitor(LeitorCSV leitor) {
        this.leitor = leitor;
    }

    public void setAvaliador(Avaliador avaliador) {
        this.avaliador = avaliador;
    }
    
    public void processar(String arqCsvEntrada, String arqCsvSaida) {
        escritor.setArquivoSaida(arqCsvSaida);
        
        List<Aluno> alunos = leitor.getAlunosDe(arqCsvEntrada);
        if(alunos.size() == 1 && alunos.get(0).getRA().startsWith("ERROR READING FILE")) {
            escritor.escrever(alunos.get(0).getRA(), alunos.get(0).getRA(), 0.0, alunos.get(0).getRA());
        }
        else {
            for(Aluno a : alunos) {
                String statusFinal = avaliador.getStatusFinal(a);
                escritor.escrever(a.getRA(), a.getNome(), a.getNF(), statusFinal);
            }
        }            
    }

    
}