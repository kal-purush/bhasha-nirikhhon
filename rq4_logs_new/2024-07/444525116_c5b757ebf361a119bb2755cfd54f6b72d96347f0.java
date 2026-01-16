package br.com.lojasquare;

import br.com.lojasquare.core.CheckService;
import br.com.lojasquare.core.autoconfig.CheckCreateGroupItem;
import br.com.lojasquare.listener.ProdutoListener;
import br.com.lojasquare.providers.lojasquare.ILSProvider;
import br.com.lojasquare.providers.request.IRequestProvider;
import br.com.lojasquare.utils.ConfigManager;
import br.com.lojasquare.utils.PluginLoadUtil;
import br.com.lojasquare.utils.SiteUtil;
import lombok.Getter;
import lombok.Setter;
import org.bukkit.Bukkit;
import org.bukkit.command.ConsoleCommandSender;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class LojaSquare extends JavaPlugin{
	
	private static LojaSquare pl;
	private static int tempoChecarItens;
	private static List<String> produtosAtivados = new ArrayList<>();
	private static List<String> produtosConfigurados = new ArrayList<>();
	private static SiteUtil ls;
	private static String servidor;
	private static boolean debug,smartDelivery;
	private static ConfigManager confGrupos;
	//
	@Getter @Setter private ILSProvider lsProvider;
	@Getter @Setter private IRequestProvider requestProvider;
	
	public void onEnable() {
		ConsoleCommandSender b = Bukkit.getConsoleSender();
		try{
			PluginLoadUtil plu = new PluginLoadUtil();
			defineVariaveisAmbiente();
			String keyapi 	= getKeyAPI();
			b.sendMessage("§6=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
			if(!checarServidorConfigurado(b)) return;
			
			b.sendMessage("§3[LojaSquare] §bAtivado...");
			b.sendMessage("§3Criador: §bTrow");
			b.sendMessage("§bDesejo a voce uma otima experiencia com o §dLojaSquare§b.");
			// Carregando todos os grupos de produtos configurados
			carregaGruposEntregaConfigurados(b);
			
			// INICIO definindo variaveis do LojaSquare
			plu.prepareWebServiceConnection(b, keyapi, getPublicAPI(), lsProvider, requestProvider, ls, pl);
			// FIM definindo variaveis do LojaSquare

			registraEventosCmds();
			
			checagensDeInicializacao(b);
			
			b.sendMessage("§6=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
		}catch (Exception e){
			e.printStackTrace();
			b.sendMessage("§4[LojaSquare] §cErro ao iniciar o plugin LojaSquare.");
			b.sendMessage("§4[LojaSquare] §cErro: §a"+e.getMessage());
			Bukkit.getPluginManager().disablePlugin(this);
		}
	}

	private void registraEventosCmds() {
		Bukkit.getPluginManager().registerEvents(new ProdutoListener(pl,lsProvider), this);
	}

	private void checagensDeInicializacao(ConsoleCommandSender b) {
		List<CheckService> checkServices = new ArrayList<>();
		checkServices.add(new CheckCreateGroupItem(pl, lsProvider));

		checkServices.forEach(s -> s.execute(b));
	}

	private void carregaGruposEntregaConfigurados(ConsoleCommandSender b) {
		confGrupos 	= new ConfigManager("produtos", pl);
		if(confGrupos == null || confGrupos.getString("Grupos") == null) return;
		b.sendMessage("§3[LojaSquare] §bIniciando o carregamento dos nomes dos grupos de itens para serem entregues...");
		for(String v : confGrupos.getConfigurationSection("Grupos").getKeys(false)){
			produtosConfigurados.add(v);
			if(!confGrupos.getBoolean("Grupos."+v+".Ativado")) {
				b.sendMessage("§4[LojaSquare] §cO grupo §a"+v+"§c nao esta ativado nas configuracoes.");
				continue;
			}
			produtosAtivados.add(v);
			b.sendMessage("§3[LojaSquare] §bGrupo carregado: §a"+v);
		}
		b.sendMessage("§3[LojaSquare] §bGrupos de entregas foram carregados com sucesso!");
	}

	private void defineVariaveisAmbiente() {
		pl=this;
		saveDefaultConfig();
		debug 				= getConfig().getBoolean("LojaSquare.Debug",true);
		servidor 			= getConfig().getString("LojaSquare.Servidor",null);
		smartDelivery 		= getConfig().getBoolean("LojaSquare.Smart_Delivery",true);
		// Definindo outras variaveis para nao ir buscar na Config.yml toda vez que for necessario.
		tempoChecarItens 	= getConfig().getInt("Config.Tempo_Checar_Compras",60);
	}

	public void onDisable() {
		ConsoleCommandSender b = Bukkit.getConsoleSender();
		b.sendMessage("§6=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
		b.sendMessage("§3[LojaSquare] §bDesativado...");
		b.sendMessage("§3Criador: §bTrow");
		b.sendMessage("§bAgradeco por usar meu(s) plugin(s)");
		b.sendMessage("§6=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
	}
	
	public boolean checarServidorConfigurado(ConsoleCommandSender b){
		if(servidor==null||servidor.equalsIgnoreCase("Nome-Do-Servidor")){
			b.sendMessage("§4[LojaSquare] §cDesativando...");
			b.sendMessage("§4[LojaSquare] §cPara que o plugin seja ativado com sucesso, e necessario configurar o nome do seu servidor na config.yml");
			b.sendMessage("§4[LojaSquare] §cAtualmente o nome do servidor esta definido como: §a"+(servidor==null?"§4NAO DEFINIDO":servidor));
			b.sendMessage("§6=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
			Bukkit.getPluginManager().disablePlugin(getInstance());
			return false;
		}
		return true;
	}
	
	public boolean canDebug(){
		return debug;
	}
	
	public static void printDebug(String s){
		if(debug){
			Bukkit.getConsoleSender().sendMessage(s);
			for(Player p:getOnlinePlayers()){
				if(p.isOp()||p.hasPermission("lojasquare.debug")){
					p.sendMessage(s);
				}
			}
		}
	}
	
	public static Player[] getOnlinePlayers() {
		try {
			Method method = Bukkit.class.getDeclaredMethod("getOnlinePlayers");
			Object players = method.invoke(null);
			if (players instanceof Player[]) {
				return (Player[]) players;
			} else {
				Collection<?> c = ((Collection<?>) players);
				return c.toArray(new Player[c.size()]);
			}

		} catch (Exception e) {
		}
		return null;
	}
	
	public int getTempoChecarItens(){
		if(tempoChecarItens<20) return 20;
		return tempoChecarItens;
	}
	
	public boolean doSmartDelivery(){
		return smartDelivery;
	}
	
	public void setSiteUtil(SiteUtil su) {
		ls = su;
	}
	
	public SiteUtil getLojaSquare(){
		return ls;
	}

	public String getKeyAPI(){
		return getMsg("LojaSquare.SECRET_API");
	}

	public String getPublicAPI(){
		return getMsg("LojaSquare.KEY_KEY");
	}
	
	public boolean produtoAtivado(String grupo){
		return produtosAtivados.contains(grupo);
	}
	
	public static List<String> getProdutosAtivados() {
		return produtosAtivados;
	}

	public static void setProdutosAtivados(List<String> produtosConfigurados) {
		produtosAtivados = produtosConfigurados;
	}

	public static List<String> getProdutosConfigurados() {
		return produtosConfigurados;
	}

	public static void setProdutosConfigurados(List<String> produtosConfigurados) {
		pl.produtosConfigurados = produtosConfigurados;
	}

	public String getMsg(String s){
		try {
			return getConfig().getString(s).replace("&", "§");
		} catch (Exception e) {
			Bukkit.broadcastMessage("§cLinha nao encontrada na config: §a"+s);
			return "";
		}
	}
	
	public void print(String s){
		Bukkit.getConsoleSender().sendMessage(s);
	}
	
	public static LojaSquare getInstance(){
		return pl;
	}

	public String getServidor() {
		return servidor;
	}

	private void setServidor(String servidor) {
		this.servidor = servidor;
	}

	public static ConfigManager getConfGrupos() {
		return confGrupos;
	}

	public static void setConfGrupos(ConfigManager confGrupos) {
		confGrupos = confGrupos;
	}
	
}