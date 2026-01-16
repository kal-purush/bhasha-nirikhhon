package school.sptech.backend.api.tipocampanha;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import school.sptech.backend.api.BaseController;
import school.sptech.backend.domain.campanha.Campanha;
import school.sptech.backend.domain.tipocampanha.TipoCampanha;
import school.sptech.backend.domain.tipocampanha.repository.TipoCampanhaRepository;
import school.sptech.backend.service.campanha.dto.CampanhaAtualizacaoDto;
import school.sptech.backend.service.campanha.dto.CampanhaCriacaoDto;
import school.sptech.backend.service.campanha.dto.CampanhaListagemDto;
import school.sptech.backend.service.tipocampanha.TipoCampanhaService;
import school.sptech.backend.service.tipocampanha.dto.TipoCampanhaAtualizacaoDto;
import school.sptech.backend.service.tipocampanha.dto.TipoCampanhaCriacaoDto;
import school.sptech.backend.service.tipocampanha.dto.TipoCampanhaListagemDto;
import school.sptech.backend.service.tipocampanha.dto.TipoCampanhaMapper;

import java.net.URI;
import java.util.List;

@RestController
@RequestMapping("tipo-campanhas")
@RequiredArgsConstructor
public class TipoCampanhaController implements BaseController<TipoCampanhaCriacaoDto, TipoCampanhaAtualizacaoDto, TipoCampanhaListagemDto, Integer> {

    private final TipoCampanhaService tipoCampanhaService;
    private final TipoCampanhaMapper tipoCampanhaMapper;

    @PostMapping
    public ResponseEntity<TipoCampanhaListagemDto> criar(@RequestBody @Valid TipoCampanhaCriacaoDto tipoCampanhaCriacaoDto) {
        TipoCampanha tipocampanhaCriada = this.tipoCampanhaService.criar(tipoCampanhaMapper.toEntity(tipoCampanhaCriacaoDto));
        URI uri = URI.create("/tipo-campanhas/" + tipocampanhaCriada.getIdTipoCampanha());
        return ResponseEntity.created(uri).body(tipoCampanhaMapper.toDto(tipocampanhaCriada));
    }

    @GetMapping
    public ResponseEntity<List<TipoCampanhaListagemDto>> listar(){
        List<TipoCampanha> tipoCampanhas = this.tipoCampanhaService.listar();
        return ResponseEntity.ok().body(tipoCampanhaMapper.toDto(tipoCampanhas));
    }

    @GetMapping("/{id}")
    public ResponseEntity<TipoCampanhaListagemDto> porId(@PathVariable Integer id){
        TipoCampanha entity = this.tipoCampanhaService.porId(id);
        return ResponseEntity.ok().body(tipoCampanhaMapper.toDto(entity));
    }

    @PutMapping("/{id}")
    public ResponseEntity<TipoCampanhaListagemDto> atualizar(@PathVariable Integer id, @RequestBody @Valid TipoCampanhaAtualizacaoDto tipoCampanhaAtualizacaoDto) {
        TipoCampanha entity = tipoCampanhaMapper.toEntity(tipoCampanhaAtualizacaoDto);
        TipoCampanhaListagemDto dto = tipoCampanhaMapper.toDto(this.tipoCampanhaService.atualizar(entity.getIdTipoCampanha(), entity));
        return ResponseEntity.ok().body(dto);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deletar(@PathVariable Integer id){
        this.tipoCampanhaService.deletar(id);
        return ResponseEntity.noContent().build();
    }
}