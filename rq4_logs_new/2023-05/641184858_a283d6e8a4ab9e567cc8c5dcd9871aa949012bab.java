package ufes.presenter;
import java.util.ArrayList;
import ufes.model.DadoClima;
import ufes.view.EstacaoClimaticaView;

public class EstacaoClimaticaPrincipalPresenter {
    private EstacaoClimaticaView estacaoClimaticaView;
    //private LogOperacoesDados logOperacoesDados;
    private ArrayList<IPainel> paineis;
    private ArrayList<DadoClima> dadosClima;
    
    public EstacaoClimaticaPrincipalPresenter(){
        estacaoClimaticaView = new EstacaoClimaticaView();
        paineis = new ArrayList();
        dadosClima = new ArrayList();
    }
    
    /*Operações com os painéis*/
    public void registrarPainel(IPainel painel){
        paineis.add(painel);
    }
    
    public void removerPainel(IPainel painel){
        paineis.add(painel);
    }
    
    /*Operações com os dados de clima*/
    public void incluirDadoClima(DadoClima dadoClima){
        dadosClima.add(dadoClima);
        atualizarMedicoes();
    }
    
    public void removerDadoClima(){
        
    }
    
    /*Operações internas*/
    public void atualizarMedicoes(){
        //DadoClima dadoClima = new DadoClima(temperatura, umidade, pressao, data);
        notificarPaineis();
    }
    
    private void notificarPaineis(){
        for(int i=0; i<paineis.size(); i++){
            paineis.get(i).atualizar(dadosClima, estacaoClimaticaView);
        }
    }
}