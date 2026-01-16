package ex_tep.minhasseries.entidades;

import io.realm.RealmObject;

/**
 * Created by Anailson on 11/10/2015.
 */
public class Configuracoes extends RealmObject{

    private int ordenacaoFav;
    private int ordenacaoSer;

    public int getOrdenacaoFav() {
        return ordenacaoFav;
    }

    public void setOrdenacaoFav(int ordenacaoFav) {
        this.ordenacaoFav = ordenacaoFav;
    }

    public int getOrdenacaoSer() {
        return ordenacaoSer;
    }

    public void setOrdenacaoSer(int ordenacaoSer) {
        this.ordenacaoSer = ordenacaoSer;
    }
}