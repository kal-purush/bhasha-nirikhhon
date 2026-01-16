package school.sptech.backend.api.relatorio;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import school.sptech.backend.domain.produtounitario.ProdutoUnitario;
import school.sptech.backend.domain.relatorio.Relatorio;
import school.sptech.backend.service.produtounitario.dto.ProdutoUnitarioMapper;
import school.sptech.backend.service.produtounitario.dto.ProdutoUnitarioRelatorioDto;
import school.sptech.backend.service.relatorio.RelatorioService;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@RestController
@RequestMapping("/relatorio")
@RequiredArgsConstructor
public class RelatorioController {

    private final RelatorioService service;
    private final ProdutoUnitarioMapper mapper;

    @PostMapping("/importar")
    public ResponseEntity<Void> inserir(@RequestParam MultipartFile file) {

        return null;
    }

    @GetMapping("/{dataInicio}/{dataFim}/{tipoArquivo}")
    public ResponseEntity<List<ProdutoUnitarioRelatorioDto>> download(@PathVariable String dataInicio, @PathVariable String dataFim, @PathVariable String tipoArquivo) {

        List<ProdutoUnitario> produtoUnitarios = service.buscaPorPeriodo(dataInicio, dataFim);
        List<ProdutoUnitarioRelatorioDto> produtoRelatorios = mapper.toDtoRelatorio(produtoUnitarios);

        List<Relatorio> relatorios = new ArrayList<>();



        String nomeArquivo;

        switch (tipoArquivo.toLowerCase(Locale.ROOT)) {
            case "csv":
                nomeArquivo = "relatorio-" + dataInicio + "-" + dataFim;
                gravarArquivoCsv(produtoRelatorios, nomeArquivo);
                break;
            case "txt":
                nomeArquivo = "relatorio-formato";
                gravaArquivoTxtRelatorio(relatorios, nomeArquivo);
                nomeArquivo = "produto-formato";
                gravaArquivoTxtProduto(produtoRelatorios, nomeArquivo);
                break;
            default:
                return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok().body(produtoRelatorios);
    }

    public static void gravarArquivoCsv(List<ProdutoUnitarioRelatorioDto> lista, String nomeArq) {
        FileWriter arq = null;
        Formatter saida = null;
        Boolean deuRuim = false;

        nomeArq += ".csv";

        try {
            arq = new FileWriter(nomeArq);
            saida = new Formatter(arq);
        }
        catch (IOException erro) {
            System.out.println("Erro ao abrir o arquivo");
            System.exit(1);
        }

        try {
            for (ProdutoUnitarioRelatorioDto produto : lista) {


                String origem = produto.getOrigem().getItapora() == 1? "Itapora" : "Auto de Suuza";

                saida.format("%30s;%10s;%4.2f;%10s,%50s\n",
                        produto.getProduto().getNome(),
                        produto.getDataValidade(),
                        produto.getPeso(),
                        produto.getUnidadeMedida().getNome(),
                        origem);
            }
        }
        catch (FormatterClosedException erro) {
            System.out.println("Erro ao gravar o arquivo");
            deuRuim = true;
        }
        finally {
            saida.close();
            try {
                arq.close();
            }
            catch (IOException erro) {
                System.out.println("Erro ao fechar o arquivo");
                deuRuim = true;
            }
            if (deuRuim) {
                System.exit(1);
            }
        }
    }


    public static void gravaArquivoTxtRelatorio(List<Relatorio> lista, String nomeArq) {
        int contaRegDados = 0;


        String header = "00";
        header += "RELATORIO";
        header += LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"));
        header += "01";
        gravaRegistro(nomeArq, header);

        String corpo = "";
        for (Relatorio r : lista) {

            corpo = "02";
            corpo += String.format("%s",r.getProduto().getNome());
            corpo += String.format("%s", r.getQtdVencido());
            corpo += String.format("%s", r.getQtdArrecadado());
            corpo += String.format("%s", r.getMesReferencia());

            gravaRegistro(nomeArq, corpo);
            contaRegDados++;
        }

        String trailer = "01";
        trailer += String.format("%010d", contaRegDados);
        gravaRegistro(nomeArq, trailer);
    }

    public static void gravaArquivoTxtProduto(List<ProdutoUnitarioRelatorioDto> lista, String nomeArq) {
        int contaRegDados = 0;


        String header = "00PRODUTO";
        header += LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"));
        header += "01";
        gravaRegistro(nomeArq, header);


        String corpo = "";
        for (ProdutoUnitarioRelatorioDto p : lista) {

            corpo = "02";
            corpo += String.format("%s",p.getProduto().getNome());
            corpo += String.format("%s", p.getDataValidade());
            corpo += String.format("%.2f", p.getPeso());
            corpo += String.format("%s", p.getUnidadeMedida());
            corpo += String.format("%s", p.getOrigem());
            gravaRegistro(nomeArq, corpo);
            contaRegDados++;
        }

        String trailer = "01";
        trailer += String.format("%010d", contaRegDados);
        gravaRegistro(nomeArq, trailer);
    }



    public static void gravaRegistro(String nomeArq, String registro) {
        BufferedWriter saida = null;

        // Abre o arquivo
        try {
            saida = new BufferedWriter(new FileWriter(nomeArq, true));
        }
        catch (IOException erro) {
            System.out.println("Erro ao abrir o arquivo: " + erro.getMessage());
        }

        // Grava o registro e fecha o arquivo
        try {
            saida.append(registro + "\n");
            saida.close();
        }
        catch (IOException erro) {
            System.out.println("Erro na gravacao do arquivo: " + erro.getMessage());
        }
    }



}