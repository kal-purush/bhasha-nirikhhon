/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Cliente;

import java.util.Scanner;
import Menu.Menu;
import NewConta.NewConta;

/**
 *
 * @author ❤🚀❤ Vitor de Jesus ❤🚀❤
 */
public class Cliente {
    
    public String Nome = "";
    public String CPF = "";
    public String RG = "";
    public String Telefone = "";
    public String Endereco = "";


    Scanner objScanner = new Scanner(System.in);

        public void cadastro () {
            
        System.out.println("######### É BOM TER VOCÊ AQUI  ##########");
        System.out.println("#                                       #");
        System.out.println("# AQUI REALIZAREMOS SEU CADASTRO !!     #");
        System.out.println("# SOLICITAREMOS ALGUNS DADOS !          #");
        System.out.println("#                                       #");
        System.out.println("#########################################");

        System.out.println("Informe seu nome completo");    
        Nome = objScanner.next();

        System.out.println("Informe seu CPF: ");    
        CPF = objScanner.next();  

        System.out.println("Informe seu RG: ");    
        RG = objScanner.next();  

        System.out.println("Informe seu Telefone: ");    
        Telefone = objScanner.next();  

        System.out.println("Informe seu Endereco: ");    
        Endereco = objScanner.next();  

        
        if("".equals(Nome) && "".equals(CPF) && "".equals(RG) && "".equals(Endereco)){

            // Criar rotas para inserir dados no Banco (Futuro);

            
            NewConta objNewConta = new NewConta();
            objNewConta.cadastro();
            
         /*   Menu objMenu = new Menu();
            objMenu.executaMenu();  */

        }
        else{
            System.out.println("Preencha todos os dados");
            return;
        }

        // ❣🚀❣ Vitor de Jesus❣🚀❣
    }  
    
}