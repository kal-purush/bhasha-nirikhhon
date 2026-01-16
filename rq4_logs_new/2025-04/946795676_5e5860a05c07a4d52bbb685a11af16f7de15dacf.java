public class SensorMovimento extends Sensor{
    public SensorMovimento(int r){
        super(r);
    }

    int consegueAvancar(int mov, int sent, int x, int y, int z, int falta, Ambiente espaco, int[][] visitados){
        //retorna a maior distancia que o robo pode avancar limitado ao raio do sensor
        //passo é a distancia (dekta) que o Robo ira se mover em uma direcao no seu percurso
        int passo;
        
        if(mov == 1){ //"mover em x"
            if(sent == 1){ //mover no sentido positivo de x
                for(passo = 1; passo <= this.getRaio() && passo <= falta; passo++){
                    if(espaco.identifica_colisao(x + passo, y, z)){//posicao invalida
                        return passo - 1;//retorna o quanto consegue avancar naquela direcao
                    }
                }
                return passo - 1;
            }
            else if(sent == 2){//mover no sentido negativo de x
                for(passo = 1; passo <= this.getRaio() && passo <= falta; passo++){ 
                    if(espaco.identifica_colisao(x - passo, y, z)){
                        return -(passo - 1);
                    }
                }
                return -(passo - 1);
            }
        }
        else if(mov == 2){ //mover em y
            if(sent == 1){//sentido positivo; delta y > 0
                for(passo = 1; passo <= this.getRaio() && passo <= falta; passo++){
                    if(espaco.identifica_colisao(x, y + passo, z)){
                        return passo - 1;
                    }
                }
                return passo - 1;
            }
            else if(sent == 2){//sentido negativo; delta y < 0
                for(passo = 1; passo <= this.getRaio() && passo <= falta; passo++){
                    if(espaco.identifica_colisao(x, y - passo, z)){
                        return -(passo - 1);
                    }
                }
                return -(passo - 1);
            }
        }
        else if(mov == 3){ // mover em z
            if(sent == 1){//sentido positivo de z
                for(passo = 1; passo <= this.getRaio() && passo <= falta; passo++){
                    if(espaco.identifica_colisao(x, y, z + passo)){
                        return passo - 1;
                    }
                }
                return passo - 1;
            }
            else if(sent == 1){//sentido negativo de z
                for(passo = 1; passo <= this.getRaio() && passo <= falta; passo++){
                    if(espaco.identifica_colisao(x, y, z - passo)){
                        return -(passo - 1);
                    }
                }
                return -(passo - 1);
            }
        }
        return 0;
    }
}