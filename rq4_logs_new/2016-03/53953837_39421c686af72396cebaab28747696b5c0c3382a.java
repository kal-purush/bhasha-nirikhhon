/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tradutorlinguagens;

import tradutorlinguagens.Observer.Observado;
import tradutorlinguagens.Observer.Observador;

/**
 *
 * @author TheOwnzs
 */
public class LinguagemFortran implements Observador {
    private String Linguagem;

    public LinguagemFortran() {
    }

    public LinguagemFortran(String Linguagem) {
        this.Linguagem = Linguagem;
    }

    public String getLinguagem() {
        return Linguagem;
    }

    public void setLinguagem(String Linguagem) {
        this.Linguagem = Linguagem;
    }    

    @Override
        public void atualizar(Object ob) {
        setLinguagem(ob.toString());
         System.out.println("Fortran");
    }
    
}
