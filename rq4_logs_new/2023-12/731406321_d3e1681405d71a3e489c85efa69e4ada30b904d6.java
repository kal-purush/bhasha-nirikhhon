package br.com.abneco.food.pagamento.api;

import br.com.abneco.food.pagamento.dto.PagamentoDto;
import br.com.abneco.food.pagamento.services.PagamentoService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;

@RestController
@RequestMapping("/abneco/food/pagamento")
public class PagamentoController {

    @Autowired
    private PagamentoService service;

    @GetMapping("")
    @ResponseStatus(HttpStatus.OK)
    public Page<PagamentoDto> fetchPayments(@PageableDefault(size = 10) Pageable pageable) {
        return service.fetchPayments(pageable);
    }

    @GetMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    public PagamentoDto fetchById(@PathVariable Long id) {
        return service.fetchById(id);
    }

    @PostMapping("")
    public ResponseEntity<PagamentoDto> registerPayment(@RequestBody @Valid PagamentoDto pagamentoDto, UriComponentsBuilder uriBuilder) {
        PagamentoDto pagamento = service.registerPayment(pagamentoDto);
        URI path = uriBuilder.path("/pagamento/{id}").buildAndExpand(pagamento.getId()).toUri();
        return ResponseEntity.created(path).body(pagamento);
    }

    @PutMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    public PagamentoDto updatePayment(@PathVariable Long id, @RequestBody PagamentoDto pagamentoDto) {
        return service.updatePayment(id, pagamentoDto);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deletePayment(@PathVariable Long id) {
        service.deletePayment(id);
    }

}