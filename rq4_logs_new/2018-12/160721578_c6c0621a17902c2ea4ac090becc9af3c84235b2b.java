package jogoAlex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class Biblio {
	
	//BIBLIOTECA DE PALAVRAS
	
	//static Set<Palavra> s = new HashSet<Palavra>();
	static Set<Palavra> s2 = new HashSet<Palavra>();
	
//	public static Set <Palavra> retornaBiblioteca(){
//		return s; 
//	}
	
//	public static Set <Palavra> inserePalavra(){
//		boolean t = false;
//		Palavra palavra = new Palavra(" ", " ");
//		String p1, p2;
//		do {
//			System.out.println("Deseja inserir nova palavra? [S/N]");
//			String c = Utils.scanf();
//			if(c.equals("N")) {
//				t = false;
//			}
//			else{
//				if(c.equals("S")) {
//					t = true;
//					System.out.println("Insira a palavra: ");
//					p1 = (String) Utils.scanf();
//					System.out.println("Insira a palavra-chave: ");
//					p2 = (String) Utils.scanf();
//					palavra = new Palavra(p1, p2);
//					s.add(palavra);
//				}				
//			}
//		}while(t);
//		return s;
//	}
	
	// A ideia era fazer com que o usuario pudesse sugerir novas palavras para o jogo
	//logo, a implementacao de leitura e insercao de dados em arquivo seria feita
	//porem, diversos fatores impediram isso :/
	
	public static ArrayList <Palavra> biblioteca() {
		ArrayList<Palavra> a = new ArrayList<Palavra>();
		a.add(new Palavra("GEOMETRIA", "MATEMATICA"));		
		a.add(new Palavra("ROMANTISMO", "LITERATURA"));
		a.add(new Palavra("CARTOGRAFIA", "GEOGRAFIA"));
		a.add(new Palavra("LEPTOSPIROSE", "DOENCAS"));
		a.add(new Palavra("LARANJEIRA", "ARVORE FRUTIFERA"));
		for(int i = 0; i<a.size(); i++) {
			s2.add(a.get(i));
		}
		List<Palavra> b = new ArrayList<Palavra>();
		Iterator<Palavra> it = s2.iterator();
		while(it.hasNext()) {
			b.add((Palavra) it.next());
		}
		return (ArrayList<Palavra>) b;
	}

//	public boolean compara(Object o, String p) {
//		if(o instanceof String) {
//			String pal = (String) o;
//			if(pal.contains(p)) {
//				return true;
//			}
//		}
//		return false;
//	}


}