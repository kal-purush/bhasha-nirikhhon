package  modelo;
import util.Registro;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

public class Ator implements Registro{
    
    private int id;
    private String nome;

    public Ator() {
        this.id = -1;
        this.nome = "";
    }

    public Ator(String nome) {
        this.id = -1; // O ID será atribuído automaticamente no CRUD
        this.nome = nome;
    }

    public int getID() {
        return this.id;
    }

    public void setID(int id) {
        this.id = id;
    }

    public String getNome() {
        return this.nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    @Override
    public String toString() {
        return "ID: " + id + "\nNome: " + nome;
    }

    // Serialização para armazenamento em arquivo
    public byte[] toByteArray() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(id);
        dos.writeUTF(nome);

        return baos.toByteArray();
    }

    // Desserialização para reconstruir objeto a partir de bytes
    public void fromByteArray(byte[] ba) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(ba);
        DataInputStream dis = new DataInputStream(bais);

        id = dis.readInt();
        nome = dis.readUTF();
    }
    
}