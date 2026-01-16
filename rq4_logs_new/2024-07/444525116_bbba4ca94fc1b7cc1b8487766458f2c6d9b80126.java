package br.com.lojasquare.qrcode.core.commands;

import br.com.lojasquare.qrcode.LojaSquare;
import br.com.lojasquare.qrcode.utils.StringUtils;
import br.com.lojasquare.qrcode.utils.enums.AcaoMenuConfirmarEnum;
import br.com.lojasquare.qrcode.utils.model.ProdutoInfoGUI;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.bukkit.entity.Player;

import java.util.Objects;

@AllArgsConstructor
@Getter
public class GerarQrCodeCmd {

    private LojaSquare pl;

    public void executar(Player sender, String ...args) {
        if(args.length < 3) {
            sender.sendMessage(pl.getMsg("Msg.Use_Cmd_Pix"));
            return;
        }
        if(!StringUtils.isNumero(args[2])) {
            sender.sendMessage(pl.getMsg("Msg.Use_Numeros_No_Campo_IdProduto_Quantidade"));
            return;
        }
        String idGrupoProduto = args[1];
        ProdutoInfoGUI produtoInfoGUI = pl.getListaProdutos().stream().filter(pInfo ->
                    pInfo.getGrupo().equalsIgnoreCase(idGrupoProduto) ||
                    pInfo.getProdutoId().toString().equals(idGrupoProduto))
                .findFirst().orElse(null);
        if(Objects.isNull(produtoInfoGUI)) {
            sender.sendMessage(pl.getMsg("Msg.Nao_Foi_Possivel_Identificar_Produto_Selecionado"));
            return;
        }
        int qtd = Integer.parseInt(args[2]);
        produtoInfoGUI.setQuantidade(qtd);
        pl.getOpenGuiConfirmar().getListaNickProduto().put(sender.getName(), produtoInfoGUI);
        AcaoMenuConfirmarEnum.GERAR_QRCODE.getAcao().executar(sender, null);
    }
}