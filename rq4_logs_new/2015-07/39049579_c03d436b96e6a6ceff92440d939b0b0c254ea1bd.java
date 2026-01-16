package Agenda;

import java.util.*;

class Compromissos {

	public static void Compromissos(){
	
	int i, recebe, visualizar;
        
        String visData;
        metodos comp[]; 
        comp = new metodos[100];
        Scanner entrada = new Scanner (System.in);
        
        System.out.println("Insira: \n1-Incluir \n2-Visualizar\n0-Sair");
        recebe = entrada.nextInt();
        
        
        
       while(recebe!=0){
    	   
    	   switch (recebe) {
		
    	   //Insere um novo compromisso
    	   case 1:
			
			entrada.nextLine();
		for (i = 0; i < comp.length; i++) { //length é um atributo público do vetor es, que define seu tamanho
          
			if(comp[i]==null){
			comp[i] = new metodos (); //INSTANCIAÇÃO DE UM OBJETO DA CLASSE COMPROMISSO NA POSIÇÃO i DO VETOR comp
           System.out.println("Compromissos "+i);
           System.out.print("Informe á Data: ");
           comp[i].setData(entrada.nextLine());
           System.out.print("Informe á Descrição: ");
           comp[i].setDescricao(entrada.nextLine());
           System.out.print("Informe á Hora: ");
           comp[i].setHora(entrada.nextLine());
           System.out.print("Informe o Local: ");
           comp[i].setLocal(entrada.nextLine());
           entrada.nextLine(); //Le o '\n', 
           break;
           }
         
		}
		
			break;
			
    	  
    		   
				//VISUALIZA TODOS OS COMPROMISSOS E COMPROMISSOS DO DIA
		case 2:
			System.out.println("Insira: \n1-Todos\n2-Por Data\n\n0-Voltar");
			visualizar = entrada.nextInt();
			
		while(visualizar!=0){
			switch (visualizar) {
				
			//VISUALIZA TODOS OS COMPROMISSOS
			case 1:
				
				System.out.println("COMPROMISSOS CADASTRADOS");
		        for (i = 0; i < comp.length; i++) {
		            if(comp[i]!=null){// VERIFICA AS POSIÇÕES  NULAS
		            	System.out.println("\n");//USADO PARA DAR ENTER APÓS EXIBIÇÃO DE CADA COMPROMISSO
		        	System.out.println("COMPROMISSO "+i);
		        	System.out.println("DATA: "+comp[i].getData());
		            System.out.println("DESCRIÇÃO: "+comp[i].getDescricao());
		            System.out.println("HORA: "+comp[i].getHora());
		            System.out.println("LOCAL: "+comp[i].getLocal());
		            
		         
		            
		            }
		            
		        }
				break;
				//EXIBE COMPROMISSOS POR DATA
			case 2:
				boolean verif=false;//USADO PARA CONFIRMAR  SE FOI ENCONTRADO ALGO NO VETOR IGUAL AO DIGITADO PELO USUARIO
				entrada.nextLine();// USADO COMO ENTER
				System.out.println("Digite o dia");
				visData = entrada.nextLine();
				i=0;
				while(comp[i]!=null){// VERIFICA SOMENTE AS POSIÇÕES DIFERENTES DE NULL
					
					//VERIFICA EM QUAIS POSIÇÕE DO VETOR POSSUE A MESMA INFORMAÇÃO QUE A DIGITADA PELO USUARIO
					if((comp[i].getData().startsWith(visData)&&(comp[i]!=null))){
						System.out.println("COMPROMISSO "+i);
				        System.out.println("DATA: "+comp[i].getData());
			            System.out.println("DESCRIÇÃO: "+comp[i].getDescricao());
			            System.out.println("HORA: "+comp[i].getHora());
			            System.out.println("LOCAL: "+comp[i].getLocal()+"\n");
			           verif=true; // SE ENCONTRADO ALGUMA INFORMAÇÃO
						 
					}
					i=i+1;
					
					
					
				}
				System.out.println("/n");
				if(verif==false){// SE NÃO FOR ENCONTRADA ALGUMA INDFORMAÇÃO 
					System.out.println("Compromisso não encontrado!");
					
				}
				break;
			
			default:
				System.out.println("Opção Incorreta!");// CASO O USUARIO TENHA DIGITADO UMA OPÇAO  ERRADA
				break;
			
			
				}
			System.out.println("Insira: \n1-Todos\n2-Por Data\n\n0-sair");
			visualizar = entrada.nextInt();
    	   }
			
		
			
		
			break;
		default:
			System.out.println("Opção Incorreta!");// CASO O USUARIO TENHA DIGITADO UMA OPÇAO  ERRADA
		
			break;
		}
    	   System.out.println("\n");//USADO PARA DAR ENTER APÓS EXIBIÇÃO DE CADA COMPROMISSO
    	   System.out.println("Insira: \n1-incluir \n2-visualizar\n0-Sair");
           recebe = entrada.nextInt();
        
       
           
           
       		}
		}

}