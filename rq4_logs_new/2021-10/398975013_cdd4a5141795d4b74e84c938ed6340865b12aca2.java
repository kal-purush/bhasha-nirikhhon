package br.edu.ufersa.problemaDaMesa;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Utils {
	
	public static List<Pessoa> listaConhecidaComConfitos() {
		
		List<Pessoa> participante = new ArrayList<Pessoa>();
		
		Pessoa iorrane = new Pessoa("iorrane");
		Pessoa danilo = new Pessoa("danilo");
		Pessoa marco = new Pessoa("marco");
		Pessoa vinicius = new Pessoa("vinicius");
		Pessoa andre = new Pessoa("Andr�");
		Pessoa victor = new Pessoa("Victor");
		Pessoa luan = new Pessoa("Luan");
		Pessoa sebastiao = new Pessoa("Sebasti�o");
		Pessoa thiago = new Pessoa("thiago");
		Pessoa bruno = new Pessoa("bruno");
		Pessoa breno = new Pessoa("breno");
		Pessoa lamark = new Pessoa("lamark");
		Pessoa alef = new Pessoa("alef");
		Pessoa joao = new Pessoa("joao");
	
		participante.add(iorrane);
		participante.add(danilo);
		participante.add(marco);
		participante.add(vinicius);
		participante.add(andre);
		participante.add(victor);
		participante.add(luan);
		participante.add(sebastiao);
		participante.add(alef);
		participante.add(joao);
		participante.add(lamark);
		participante.add(breno);
		participante.add(bruno);
		participante.add(thiago);
		
		iorrane.adicionaInimigo(danilo);
		iorrane.adicionaInimigo(marco);
		iorrane.adicionaInimigo(andre);
		iorrane.adicionaInimigo(luan);
		danilo.adicionaInimigo(vinicius);
		danilo.adicionaInimigo(andre);
		andre.adicionaInimigo(luan);
		sebastiao.adicionaInimigo(victor);
		sebastiao.adicionaInimigo(vinicius);
		andre.adicionaInimigo(victor);
		breno.adicionaInimigo(joao);
		joao.adicionaInimigo(lamark);
		lamark.adicionaInimigo(alef);
		alef.adicionaInimigo(thiago);
		thiago.adicionaInimigo(bruno);
		bruno.adicionaInimigo(sebastiao);
		
		return participante;
		
	}
	
	public static List<Pessoa> listaConhecida(){
		List<Pessoa> participante = new ArrayList<Pessoa>();
		
		Pessoa iorrane = new Pessoa("iorrane");
		Pessoa danilo = new Pessoa("danilo");
		Pessoa marco = new Pessoa("marco");
		Pessoa vinicius = new Pessoa("vinicius");
		Pessoa andre = new Pessoa("Andr�");
		Pessoa victor = new Pessoa("Victor");
		Pessoa luan = new Pessoa("Luan");
		Pessoa sebastiao = new Pessoa("Sebasti�o");
		Pessoa thiago = new Pessoa("thiago");
		Pessoa bruno = new Pessoa("bruno");
		Pessoa breno = new Pessoa("breno");
		Pessoa lamark = new Pessoa("lamark");
		Pessoa alef = new Pessoa("alef");
		Pessoa joao = new Pessoa("joao");
	
		participante.add(iorrane);
		participante.add(danilo);
		participante.add(marco);
		participante.add(vinicius);
		participante.add(andre);
		participante.add(victor);
		participante.add(luan);
		participante.add(sebastiao);
		participante.add(alef);
		participante.add(joao);
		participante.add(lamark);
		participante.add(breno);
		participante.add(bruno);
		participante.add(thiago);
		
		return participante;
	}

	public static List<Pessoa> mesaAleatoria() {
		List<Pessoa> list = new ArrayList<Pessoa>();
		for (int i = 0; i < 20; i++) {
			list.add(new Pessoa("P" + i));
		}
		geradorDeConflitos(list);
		return list;
	}

	public static void geradorDeConflitos(List<Pessoa> list) {
		int tamanhoLista = list.size();
		for (int i = 0; i < tamanhoLista; i++) {
			Random random = new Random();
			Pessoa p1 = list.get(random.nextInt(tamanhoLista - 1));
			Pessoa p2 = list.get(random.nextInt(tamanhoLista - 1));
			while (list.get(i).getListaInimigos().contains(p1) || list.get(i).getListaInimigos().contains(p2)
					|| p1.equals(p2) || list.get(i).equals(p2) || list.get(i).equals(p1)) {
				p1 = list.get(random.nextInt(tamanhoLista - 1));
				p2 = list.get(random.nextInt(tamanhoLista - 1));
			}
			list.get(i).adicionaInimigo(p1);
			list.get(i).adicionaInimigo(p2);
		}
	}

	public static List<Pessoa> copia(List<Pessoa> participante) {
		// TODO Auto-generated method stub
		List<Pessoa> list = new ArrayList<Pessoa>();
		for (Pessoa p : participante) {
			list.add(p);
		}
		return list;
	}

}