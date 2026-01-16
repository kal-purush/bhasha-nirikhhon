
package Visu;

/**Classe de Arestas da malha */
public class mfAresta {

     private int[] reta = new int[2];
  private int _celula;

  public void addReta(int v1, int v2){
   reta[0]=v1;
   reta[1]=v2;
  }

  public void addCelula( int cell){
    _celula=cell;}

  public int getCelula(){
    return _celula;}

  public boolean isReta(int v1, int v2){
    if(reta[0]==v1 && reta[1]==v2) {
            return true;
        }
    else {
            return false;
        }}

  public int[] getReta(){
    return reta;}

  
}