/**
 * Created by rama on 9/23/17.
 */
public class Persegi {
    int sisi;
    int sisi2;
    int luas;
    public Persegi(int sisi){
        this.sisi=sisi;
        int luas = this.sisi*2;
    }

    public Persegi(int sisi,int sisi2){
        this.sisi=sisi;
        this.sisi2=sisi2;
    }

    public int Luas(){
        return this.luas;
    }


}