import java.util.Random;

class Test{
    public static void main(String[] args) {
        NeuralNetwork brain = new NeuralNetwork(4,4,1, 0.15);



//        for(int i=0;i<50000;i++){ //construção de vetores de treino

        double difEven = 1, difOdd = 1;
        while(difEven > 0.05 && difOdd > 0.05){//treinar até o erro ser baixinho

            //construção de vetores de treino
            double[] input = new double[4];
            double sum = 0;
            for(int j=0;j<4;j++){
                input[j] = random();
                sum += input[j];
            }

            //System.out.println(input[0] + "," + input[1] + "," + input[2] + "," + input[3] + "  ->" + sum);

            if(sum % 2 == 0){
                brain.train(input, 0);
            }
            else{
                brain.train(input, 1);
            }

            //dois casos de testes para ver se é preciso continuar a treinar
            double result;
            input[0]=0;
            input[1]=0;
            input[2]=1;
            input[3]=0;
            Matrix inputM = new Matrix(input);
            result = brain.feedForward(inputM);
            difOdd = 1 - result;
            input[0]=0;
            input[1]=0;
            input[2]=1;
            input[3]=1;
            inputM = new Matrix(input);
            difEven = brain.feedForward(inputM);

        }

        //vários casos testes finais (não influenciam a rede)
        for(int i=0; i<5; i++){
            //construção de vários vetores
            double[] test = new double[4];
            for(int j=0;j<4;j++){
                test[j] = random();
            }

            Matrix testM = new Matrix(test);
            System.out.printf("%.0f,%.0f,%.0f,%.0f -- >",test[0],test[1],test[2],test[3]);
            System.out.printf("%.02f",brain.feedForward(testM));
            System.out.println();

        }
    }

    private static double random(){
        Random r= new Random();
        double nr=r.nextDouble();
        if(nr>=0.5) return 1;
        else return 0;
    }
}