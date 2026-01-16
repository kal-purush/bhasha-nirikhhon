import java.util.ArrayList;

public class Ambiente {
    private final int X;
    private final int Y;
    private final int Z;
    private final int X_obstaculo;
    private final int Y_obstaculo;
    public final ArrayList<Robo> robos;

    public Ambiente(int x, int y, int z, int x_obstaculo, int y_obstaculo) {
        //inicializa o Ambiente atribuindo valores as dimensoes x e y e cria um array vazio de robos
        this.X = x;
        this.Y = y;
        this.Z = z;
        this.X_obstaculo = x_obstaculo;
        this.Y_obstaculo = y_obstaculo;
        this.robos = new ArrayList<>();
    }

    public boolean eh_obstaculo(int x, int y){
        //retorna true se as coordenadas x e y no parametro correspondem a localizacao x e y do obstaculo no ambiente
        return (x == this.X_obstaculo && y == this.Y_obstaculo);
    }

    public int getXobstaculo(){
        return this.X_obstaculo;
    }

    public int getYobstaculo(){
        return this.Y_obstaculo;
    }


    public boolean dentroDosLimites(int x, int y, int z) {
        //retorna true se o robo esta dentro do ambiente e false caso contrario
        return (x >= 0 && this.X >= x && this.Y >= y && y >= 0 && z >=0 && this.Z >= z);

    }

    public void adicionaRobo(Robo robo){
        //adiciona um objeto da classe robo ao array de robos
        this.robos.add(robo);
    }

    public int getArrayTamanho() {
        return this.robos.size();
    }

    public int getAmbienteX(){
        return this.X;
    }

    public int getAmbienteY(){
        return this.Y;
    }

    public int getAltura() {
        return this.Z;
    }

    public Robo getRobo(int pos){
        return this.robos.get(pos);
    }

}