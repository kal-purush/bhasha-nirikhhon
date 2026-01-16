package br.com.lojasquare.qrcode.core.gui;

import br.com.lojasquare.qrcode.LojaSquare;
import br.com.lojasquare.qrcode.utils.Constants;
import br.com.lojasquare.qrcode.utils.StringUtils;
import br.com.lojasquare.qrcode.utils.bukkit.GUIAPI;
import br.com.lojasquare.qrcode.utils.bukkit.Item;
import br.com.lojasquare.qrcode.utils.bukkit.NbtItem;
import br.com.lojasquare.qrcode.utils.model.ProdutoInfoGUI;
import de.tr7zw.nbtapi.NBTItem;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

@Getter
@AllArgsConstructor
public class OpenGuiConfirmar {

    private LojaSquare pl;
    private HashMap<String, ProdutoInfoGUI> listaNickProduto;

    public void execute(Player p, ProdutoInfoGUI produtoInfoGUI) {
        listaNickProduto.remove(p.getName());

        int tamanho = pl.getConfig().getInt("GUI.Confirmar.Tamanho",18);
        if(tamanho < 9) {
            p.sendMessage("§4[LsQrCode] §cAvise o administrador que o menu esta com uma configuracao errada no tamanho do GUI.");
            return;
        }
        GUIAPI gui = new GUIAPI(tamanho, pl.getMsg("GUI.Confirmar.Nome"));

        adicionaItemEscolhidoCompra(p,produtoInfoGUI, gui);
        adicionaItemMenuAlteraQuantidade("Aumentar_Quantidade", gui);
        adicionaItemMenuAlteraQuantidade("Diminuir_Quantidade", gui);
        adicionaItemConfirmarVoltar("Confirmar_GerarQrcode", gui);
        adicionaItemConfirmarVoltar("Voltar_Menu_Principal", gui);
        completarMenuGUISlotsVazios(gui);

        p.openInventory(gui.getInventory());
        listaNickProduto.put(p.getName(), produtoInfoGUI);
    }

    private void completarMenuGUISlotsVazios(GUIAPI gui) {
        if(pl.getConfig().getBoolean("GUI.Confirmar.Completar_GUI")) {
            ItemStack is = Item.getItemStack(pl.getMsg("GUI.Confirmar.Item_ID_Completar_GUI"));
            ItemMeta im = is.getItemMeta();
            im.setDisplayName("§8AC");
            is.setItemMeta(im);

            if(pl.isBukkitVersionAcima18()) {
                NBTItem nb = NbtItem.getNbtItem(is);
                nb.setBoolean(Constants.KEY_ITEM_NBTAPI_QRCODE_AUTO_COMPLETE_GUI, true);
                is = nb.getItem();
            }
            gui.completSlotEmpty(is);
        }
    }

    private void adicionaItemMenuAlteraQuantidade(String tipoItem, GUIAPI gui) {
        String prefix = "GUI.Confirmar.Item_"+tipoItem;
        int slot = pl.getConfig().getInt(prefix+".Slot");
        int linha = pl.getConfig().getInt(prefix+".Linha");
        ItemStack item = getItemStack(prefix);

        if (pl.isBukkitVersionAcima18()) {
            NBTItem nb = NbtItem.getNbtItem(item);
            nb.setBoolean(Constants.KEY_ITEM_NBTAPI_QRCODE_ALTERA_QTD, true);
            nb.setBoolean(Constants.KEY_ITEM_NBTAPI_QRCODE_ADD_QTD, tipoItem.equals("Aumentar_Quantidade"));
            item = nb.getItem();
        }

        gui.setItem(slot,linha, item);
    }

    private void adicionaItemConfirmarVoltar(String tipoItem, GUIAPI gui) {
        String prefix = "GUI.Confirmar.Item_"+tipoItem;
        ItemStack item = getItemStack(prefix);

        int slot = pl.getConfig().getInt(prefix+".Slot");
        int linha = pl.getConfig().getInt(prefix+".Linha");

        if (pl.isBukkitVersionAcima18()) {
            NBTItem nb = NbtItem.getNbtItem(item);
            nb.setBoolean(Constants.KEY_ITEM_NBTAPI_QRCODE_CONFIRMA_QRCODE, tipoItem.equals("Confirmar_GerarQrcode"));
            nb.setBoolean(Constants.KEY_ITEM_NBTAPI_QRCODE_VOLTAR_MENU_ANTERIOR, tipoItem.equals("Voltar_Menu_Principal"));
            item = nb.getItem();
        }

        gui.setItem(slot,linha, item);
    }

    private void adicionaItemEscolhidoCompra(Player p, ProdutoInfoGUI produto, GUIAPI gui) {
        String prefix = "GUI.Confirmar.Item_Escolhido_Compra";
        int slot = pl.getConfig().getInt(prefix+".Slot");
        int linha = pl.getConfig().getInt(prefix+".Linha");
        ItemStack item = produto.getItem();
        ItemMeta itemMeta = item.getItemMeta();
        List<String> lore = new ArrayList<>();
        for(String s : itemMeta.getLore()) {
            lore.add(s.replace("@player", p.getName())
                    .replace("@produto", produto.getProduto())
                    .replace("@qtd", produto.getQuantidade()+"")
                    .replace("@valor", StringUtils.formatar(produto.getValorSemFormatacao() * produto.getQuantidade())));
        }
        itemMeta.setLore(lore);
        item.setItemMeta(itemMeta);

        if (pl.isBukkitVersionAcima18()) {
            NBTItem nb = NbtItem.getNbtItem(item);
            item = nb.getItem();
        }

        produto.setSlot(slot);
        produto.setLinha(linha);
        gui.setItem(slot,linha, item);
    }

    private ItemStack getItemStack(String prefix) {
        ItemStack item = Item.getItemStack(pl.getMsg(prefix +".ID"));
        ItemMeta itemMeta = item.getItemMeta();
        List<String> lore = new ArrayList<>();
        for(String s : pl.getConfig().getStringList(prefix +".Lore")) {
            lore.add(s.replace("&", "§"));
        }
        itemMeta.setLore(lore);
        itemMeta.setDisplayName(pl.getMsg(prefix+".Nome"));
        item.setItemMeta(itemMeta);
        return item;
    }

}