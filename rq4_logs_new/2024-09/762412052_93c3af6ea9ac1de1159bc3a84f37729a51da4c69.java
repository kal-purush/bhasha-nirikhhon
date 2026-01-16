package sptech.school.projetovolt.api.whatsapp.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import sptech.school.projetovolt.entity.produto.Produto;
import sptech.school.projetovolt.service.produto.ProdutoService;

@RestController
@RequiredArgsConstructor
@RequestMapping("/whatsapp")
@Tag(name = "Whatsapp", description = "Responsável pelo envio e direcionamento de mensagens para o Whatsapp")
public class WhatsappController {

    private final ProdutoService produtoService;

    @GetMapping()
    @Operation(summary = "Cria o link responsável por fazer o envio da mensagem", method = "POST", description = "Responsável por enviar mensagem para o Whatsapp", tags = {"Whatsapp"})
    public ResponseEntity<String> enviarMensagem(@RequestParam int idProduto) {
        Produto produtoEncontrado = produtoService.buscarProdutoPorId(idProduto);

        if (produtoEncontrado == null) return ResponseEntity.notFound().build();

        String mensagem = "Ol%C3%A1,%20gostaria%20de%20saber%20mais%20sobre%20o%20produto%20*" + produtoEncontrado.getNome() + "*!%0A" +
                "Descri%C3%A7%C3%A3o:%20" + produtoEncontrado.getDescricao() + "%0A" +
                "Pre%C3%A7o:%20R$" + produtoEncontrado.getPreco() + "%0A" +
                "Categoria:%20" + produtoEncontrado.getCategoria().getNome();

        /* TODO
         *  Remover o número fixo e adicionar um número dinâmico. O número fixo é apenas para fins de teste.
         *  O número dinâmico pode ser obtido através de um usuário Admin no sistema.
         */
        String link = "https://api.whatsapp.com/send?phone=5515992019544&text=" + mensagem;

        return ResponseEntity.ok(link);
    }

}