package ufes.presenter;

import java.time.LocalDate;
import java.util.ArrayList;
import ufes.view.EstacaoClimaticaView;
import org.jfree.data.category.DefaultCategoryDataset;
import ufes.model.DadoClima;
import ufes.model.EstacaoClimatica;

public class EstacaoClimaticaPresenter {
    private EstacaoClimatica estacaoClimatica; 
    private EstacaoClimaticaView estacaoClimaticaView;
    
    public EstacaoClimaticaPresenter(EstacaoClimatica estacaoClimatica) {
        this.estacaoClimatica = estacaoClimatica;
        estacaoClimaticaView = new EstacaoClimaticaView();
        atualizarGraficoMaxMin();
        estacaoClimaticaView.setVisible(true);
    }
    
    public void atualizarGraficoMaxMin(){
        //Preenche os dados do gráfico
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        // Preencha os dados no dataset
        dataset.addValue(24, "Máximas", "Temperatura");
        dataset.addValue(20, "Mínimas", "Temperatura");
        dataset.addValue(12, "Máximas", "Umidade");
        dataset.addValue(8, "Mínimas", "Umidade");
        dataset.addValue(32, "Máximas", "Pressão");
        dataset.addValue(16, "Mínimas", "Pressão");

        // Exiba o gráfico na view
        estacaoClimaticaView.exibirGrafico(dataset);
    }
}