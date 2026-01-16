package jogoAlex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

public class Biblio {
	
	static Scanner sc = new Scanner(System.in);
	
	//BIBLIOTECA DE PALAVRAS
	
	static Set<Palavra> lib = new HashSet<Palavra>();
	static Set<Palavra> s2 = new HashSet<Palavra>();
	 
	
	public static ArrayList <Palavra> retornaBiblioteca(){
		ArrayList <Palavra> biblioteca = biblio1();
		return biblioteca; 
	}
	
	public static ArrayList <Palavra> inserePalavra(){
		boolean fim = false;
		Palavra palavra = new Palavra(" ", " ");
		String p1, p2;
		char s = 'S';
		char n = 'N';
		while(fim==false) {
			System.out.println("Deseja inserir nova palavra? [S/N]");
			char c = Utils.leitura();
			if(c == n) {
				fim = false;
			}
			else{
				if(c == s) {
					fim = true;
					System.out.println("Insira a palavra: ");
					p1 = sc.nextLine();
					sc.close();
					System.out.println("Insira a palavra-chave: ");
					p2 = sc.nextLine();
					sc.close();
					palavra = new Palavra(p1, p2);
					lib.add(palavra);
				}				
			}
		}
		ArrayList<Palavra> lib2 = (ArrayList<Palavra>) lib;
		return lib2;
	}
	
	// A ideia era fazer com que o usuario pudesse sugerir novas palavras para o jogo
	//logo, a implementacao de leitura e insercao de dados em arquivo seria feita
	//porem, diversos fatores impediram isso :/
	
	public static ArrayList <Palavra> biblio1() {
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