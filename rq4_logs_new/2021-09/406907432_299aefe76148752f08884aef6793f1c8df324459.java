package br.com.zup.TaDentro.formularioIndicacao;

import br.com.zup.TaDentro.Usuario.Usuario;
import br.com.zup.TaDentro.Usuario.UsuarioService;
import br.com.zup.TaDentro.colaborador.Colaborador;
import br.com.zup.TaDentro.colaborador.ColaboradorService;
import br.com.zup.TaDentro.indicacao.Indicacao;
import br.com.zup.TaDentro.indicacao.IndicacaoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.validation.constraints.Size;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

@Service
public class FormularioService {


    private UsuarioService usuarioService;
    private ColaboradorService colaboradorService;
    private IndicacaoService indicacaoService;

    @Autowired
    public FormularioService(UsuarioService usuarioService, ColaboradorService colaboradorService, IndicacaoService indicacaoService) {
        this.usuarioService = usuarioService;
        this.colaboradorService = colaboradorService;
        this.indicacaoService = indicacaoService;
    }

    //Metódo formulario
    public List<Indicacao> pesquisaPorData(String email, String dataInicial, String dataFinal){

        Usuario usuario = usuarioService.encontrarUsuarioPorEmail(email);
        Optional<Colaborador> colaborador = Optional.ofNullable(colaboradorService.buscarColaboradorPorUsuario(usuario));

        return indicacaoService.pesquisarIndicacao(colaborador.get(), dataInicial, dataFinal);

    }







}