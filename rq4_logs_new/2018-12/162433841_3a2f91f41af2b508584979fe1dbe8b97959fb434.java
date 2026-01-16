package br.rpg.com;


import java.util.*;


public class Arqueiro extends Personagem{
	
    Random ran = new Random();
    Mapa mapa = new Mapa();
    Scanner in = new Scanner(System.in);
    Equipe eq = new Equipe();
    
	public Arqueiro(String name, int posX, int posY){
		super(posX,posY, name);
                calculaAtributo();
                alcanceDeAtaque = 5;
	        locomocao = 9;
                tipo="Arqueiro";
                tipoAtaque = "Tiro certo";
                tipoDefesa = "Camuflagem";
                
        }
        
        public void calculaAtributo(){
            
            do{
                vigor = 15 + ran.nextInt(50);
                agilidade = 30 + ran.nextInt(70);
                inteligencia = 10 + ran.nextInt(50);
                
            }while((agilidade < vigor)||(agilidade < inteligencia) || ((vigor+agilidade+inteligencia) >200));
            
        }
        
        public int PontoVida(){
            
            return 35 * (1+ vigor/50)+(agilidade/100)+(inteligencia/75);
        }
        
        public double Dano(){
            
            return 12*(agilidade/50)*(1+(0.1+(0.7 *ran.nextDouble())));
            
        }
        
        public double TiroCerteiro(){
            return Dano()* 1.25;
        }
        
         public double Camuflagem(Personagem defensor,Personagem atacante){
            
            double camuflagem=0;
            
           if(defensor instanceof Arqueiro ){
            if(atacante instanceof Guerreiro){
                camuflagem=(double)defensor.PontoVida()-atacante.Dano()*0;
               }
            else{
                if(atacante instanceof Mago){
                    camuflagem= (double)defensor.PontoVida()-atacante.Dano()*0;
                   }
                else{
                    if(atacante instanceof Arqueiro){
                        camuflagem= (double)defensor.PontoVida()-atacante.Dano()*0;
                    }
                }
            }
           }
        return camuflagem;
        }
         
        public void imprimirSituaçãoAtual( ArrayList<Elemento> equipe){
                System.out.println("Tipo: Arqueiro");
                System.out.println("Nome: " +name);
                System.out.println("Equipe: "+eq.getName());
                System.out.println("Ponto de Vida: "+PontoVida());
                System.out.println("Magias de ataque: TiroCerteiro"  );
                System.out.println("Magia de defesa:  Camuflagem" );
            }
        
        public boolean minhasMagias(int magia){
            
            boolean manobra=false;
            double prob;
            
            switch (magia) {
                case 1:
                    prob = 1 + ran.nextInt(100);
                    if(prob < 16){
                    manobra=true;    
                    System.out.println("Voce esta sob efeito do Tiro Certeiro");
                    }
                    break;
                case 2:
                    prob = 1 + ran.nextInt(100);
                    if(prob < 4){
                    manobra=true;    
                    System.out.println("Voce está Camuflado");
                    }
                    break;
        
        }
            return manobra;
        }    
}