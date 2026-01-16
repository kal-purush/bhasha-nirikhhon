package br.com.lojasquare.qrcode.listener;

import br.com.lojasquare.qrcode.LojaSquare;
import br.com.lojasquare.qrcode.api.ProductPreActiveEvent;
import br.com.lojasquare.qrcode.providers.lojasquare.ILSProvider;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;

@AllArgsConstructor
public class ProdutoListener implements Listener{
	
	@Getter private LojaSquare pl;
	@Getter private ILSProvider lsProvider;
	
	@EventHandler
	public void preActive(final ProductPreActiveEvent e){
		pl.printDebug("§3[LSQrCode] §bpreActive");
		if(e.isCancelled()) return;

	}
	
}