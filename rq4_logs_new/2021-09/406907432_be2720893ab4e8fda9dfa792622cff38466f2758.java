package br.com.zup.TaDentro.formularioIndicacao;

import br.com.zup.TaDentro.colaborador.ColaboradorService;
import br.com.zup.TaDentro.indicacao.Indicacao;
import br.com.zup.TaDentro.indicacao.dtos.IndicacaoPesquisaDto;
import br.com.zup.TaDentro.indicacao.dtos.IndicacaoResumidaDTO;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/formulario")
public class FormularioController {

    @Autowired
    private FormularioService formularioService;
    @Autowired
    private ModelMapper modelMapper;


    @GetMapping
    public List<IndicacaoResumidaDTO> indicacaoList(@RequestParam(required = false) String dataInicial,
                                                    @RequestParam(required = false) String dataFinal,
                                                    Authentication authentication){
        String email = authentication.getName();
        List<Indicacao> retorno =
                formularioService.pesquisaPorData(email, dataInicial,dataFinal);

        List<IndicacaoResumidaDTO> dtos = retorno
                .stream()
                .map(dto -> modelMapper.map(dto, IndicacaoResumidaDTO.class))
                .collect(Collectors.toList());
        return dtos;

    }

}