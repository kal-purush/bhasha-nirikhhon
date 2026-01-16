package edu.dhbw.andar.pub;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;

/**
 * Created by Lidiane on 25/11/2015.
 */
public class ModelVertexBuffer {

    public FloatBuffer modelVertexBuffer; //um FloatBuffer contendo coordenadas, cores e normais sequenciais para cada vértice
    int bufferSize; //(vertex_size_data + color_size_data + normal_size_data)* numVertexModel
    int fatorNormal; //multiplicador do vetor normal para determinar seu sentido

    static final int BYTES_PER_FLOAT = 4;
    static final int POSITION_DATA_SIZE = 3;
    static final int COLOR_DATA_SIZE = 3;
    static final int NORMAL_DATA_SIZE = 3;
    static final int TOTAL_DATA_SIZE = POSITION_DATA_SIZE + COLOR_DATA_SIZE + NORMAL_DATA_SIZE;

    public ModelVertexBuffer(int bufferSize, int fatorNormal){
        this.bufferSize = bufferSize;
        this.fatorNormal = fatorNormal;

        modelVertexBuffer = allocateFloatBuffer(bufferSize * BYTES_PER_FLOAT);
    }

    public static FloatBuffer allocateFloatBuffer(int capacity){
        ByteBuffer vbb = ByteBuffer.allocateDirect(capacity);
        vbb.order(ByteOrder.nativeOrder());
        return vbb.asFloatBuffer();
    }

    public void preencheVertice(Vetor ponto){
        this.modelVertexBuffer.put(ponto.x);
        this.modelVertexBuffer.put(ponto.y);
        this.modelVertexBuffer.put(ponto.z);
    }

    public void preencheCor(Vetor cor){
        this.modelVertexBuffer.put(cor.x);
        this.modelVertexBuffer.put(cor.y);
        this.modelVertexBuffer.put(cor.z);
    }

    public void preencheNormal(Vetor normal) {
        this.modelVertexBuffer.put((normal.x * fatorNormal));
        this.modelVertexBuffer.put((normal.y * fatorNormal));
        this.modelVertexBuffer.put((normal.z * fatorNormal));
    }

    public void preencheModelVertexBuffer (Vetor a, Vetor b, Vetor c, Vetor d, Vetor cor, Vetor normalInferior, Vetor normalSuperior){
        if(fatorNormal == 1){ //se a normal eh para fora, superficie externa, sentido anti horario
            /** TRIANGULO INFERIOR **/
            //primeiro vertice
            preencheVertice(a);
            preencheCor(cor);
            preencheNormal(normalInferior);

            //segundo vertice
            preencheVertice(b);
            preencheCor(cor);
            preencheNormal(normalInferior);

            //terceiro vertice
            preencheVertice(c);
            preencheCor(cor);
            preencheNormal(normalInferior);

            /** TRIANGULO SUPERIOR **/
            //primeiro vertice
            preencheVertice(c);
            preencheCor(cor);
            preencheNormal(normalSuperior);

            //segundo vertice
            preencheVertice(b);
            preencheCor(cor);
            preencheNormal(normalSuperior);

            //terceiro vertice
            preencheVertice(d);
            preencheCor(cor);
            preencheNormal(normalSuperior);
        }
        if (fatorNormal == -1){ //se a normal eh para dentro, superficie interna, sentido horario
            /** TRIANGULO INFERIOR **/
            //primeiro vertice
            preencheVertice(a);
            preencheCor(cor);
            preencheNormal(normalInferior);

            //segundo vertice
            preencheVertice(c);
            preencheCor(cor);
            preencheNormal(normalInferior);

            //terceiro vertice
            preencheVertice(b);
            preencheCor(cor);
            preencheNormal(normalInferior);

            /** TRIANGULO SUPERIOR **/
            //primeiro vertice
            preencheVertice(b);
            preencheCor(cor);
            preencheNormal(normalSuperior);

            //segundo vertice
            preencheVertice(c);
            preencheCor(cor);
            preencheNormal(normalSuperior);

            //terceiro vertice
            preencheVertice(d);
            preencheCor(cor);
            preencheNormal(normalSuperior);
        }
    }

    public int getNumModelVertex(){
        return bufferSize/TOTAL_DATA_SIZE;
    }
}