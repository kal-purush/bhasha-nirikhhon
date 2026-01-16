package regras;

import java.util.ArrayList;
import java.util.List;

import javax.swing.JFrame;
import javax.swing.JOptionPane;

public class Inscricao {
	private String playerID;
	private List<Modalidade> listadeModalidade;
	
	
	public Inscricao(){
		listadeModalidade = new ArrayList<Modalidade>();
	}
	public String getPlayerID() {
		return playerID;
	}
	public void setPlayerID(String double1) {
		this.playerID = double1;
	}
	public List<Modalidade> getListadeModalidade() {
		return listadeModalidade;
	}
	public void inserirModalidade(Double modalidadeID,String modalidadeNome){
		listadeModalidade.add(new Modalidade(modalidadeID,modalidadeNome));
	}
	public void inserirTorneio(String nomeModalidade,Double torneioID,String torneioNome,int torneioDificuldade,int nota){
		for(Modalidade mod : listadeModalidade){	//Percorrer a lista de Modalidades
			if(mod.getModalidadeNome().equals(nomeModalidade)){	//Achado a modalidade do torneio
				if(mod.checkTorneioName(torneioNome))
					JOptionPane.showMessageDialog(new JFrame("Cadastro n�o autorizado"), "Voc� j� se cadastrou nesse torneio.");
				else
					mod.getListaTorneio().add(new Torneio(torneioID,torneioNome,torneioDificuldade,nota)); 	//Adiciona o torneio na lista
																										//torneio da modalidade
			}	
		}
	}
	public void test(){
		for(Modalidade v: listadeModalidade)
			for(Torneio t: v.getListaTorneio())
				System.out.println("CPF: "+playerID+" Modalidade: "+v.getModalidadeNome()+
						"Torneio name:"+t.getNome());
	}
	public void makeTupla(){
		
		for(Modalidade m: listadeModalidade){
			double maiorNota = 0;
			Torneio temp = null;
			for(Torneio t : m.getListaTorneio()){
				double tempNota = (1/t.getMarca())*t.getDificiculdade();
				if( tempNota > maiorNota)
					temp = t;
					maiorNota = tempNota;
			}
			List<Torneio> newL = new ArrayList<Torneio>();
			newL.add(temp);
			m.setListaTorneio(newL);
		}
		return ;
	}
}