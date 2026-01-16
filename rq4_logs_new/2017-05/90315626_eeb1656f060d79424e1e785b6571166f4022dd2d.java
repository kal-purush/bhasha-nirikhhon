
package sistemadearquivos;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class Servidor2 {
    
    static ServerSocket serversocket;
    static Socket controlador_socket;
    static Conexao c;
    public static String pasta;
    private static String OS = System.getProperty("os.name").toLowerCase();
    
    // cada servidor tem uma pasta e uma porta separadas

    public Servidor2 (int porta) {
        try {
            serversocket = new ServerSocket(porta);
            System.out.println("Engines On!!! " + porta);
            System.out.println("Waiting resquest...");
        } catch (Exception e){
            System.out.println("Nao criei o server socket...");
        }
        
        if ("linux".equals(OS)){
            pasta = "/home/carlos/testes_java/server2";
        } else {
            pasta = "C:" + File.separator + "Users" + File.separator + "lab653" + File.separator + "Documents" + File.separator + "testes_java";
        }      
    }
    
    public static void main(String args[]){
        Requisicao req;
        Resposta resp;       

        new Servidor2 (9602);
        
        while(true){
            int option;
            String[] lista;
            String nome;
            String FileName, FileBody;
            Charset utf8 = StandardCharsets.UTF_8;
            List<String> lines;
            
            if(connect()){                
                
                req = (Requisicao) c.receive(controlador_socket);
                option = req.getOption();
                
                resp = new Resposta();
                
                File file = new File(pasta);
                
                //se a pasta não existir ele cria
                if (!file.exists()){
                    file.mkdirs();
                }
                
                switch (option) {
                    case 1:
                        //lista
                        
                        lista = file.list();
                        
                        resp.setLista(lista);
                        resp.setStatus(0);
                        
                        c.send(controlador_socket, resp);
                        break;
                    case 2:
                        //remover
                        
                        nome = req.getNome();
                        lista = file.list();
                        
                        for (int i = 0; i < lista.length; i++) {
                            if (nome.equals(lista[i])){
                                //deleta o arquivo
                                file = new File(pasta + File.separator + nome);
                                if (file.delete()){
                                    resp.setStatus(0);
                                }
                                break;
                            } else {
                                //mensagem arquivo não encontrado
                                resp.setStatus(3);
                            }
                        }
                        
                        c.send(controlador_socket, resp);
                        
                        break;
                    case 3:
                        //gravar
                        
                        FileName = req.getNome();
                        pasta += File.separator + FileName;
                        
                        
                        lines = Arrays.asList(req.getConteudo());

                        try {
                            Files.write(Paths.get(pasta), lines, utf8);                            
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        
                        c.send(controlador_socket, resp);

                        break;
                    case 4:
                        //ler
                        
                        nome = req.getNome();
                        lista = file.list();
                        FileBody = "";

                        for (int i = 0; i < lista.length; i++) {
                            if (!nome.equals(lista[i])){
                                //mensagem arquivo não encontrado
                                resp.setStatus(3);
                            } else {
                                System.out.println("found!!");
                                try{
                                    BufferedReader br = new BufferedReader(new FileReader(pasta + File.separator + nome));
                                    while(br.ready()){
                                        String linha = br.readLine();
                                        FileBody += linha + "\n";                                        
                                    }
                                    br.close();
                                    
                                    resp.setNome(nome);
                                    resp.setConteudo(FileBody);
                                    resp.setStatus(0);
                                    
                                }catch(IOException ioe){
                                    ioe.printStackTrace();
                                }                                 
                            }   
                        }
                        
                        c.send(controlador_socket, resp);
                        
                        break;
                    case 5:
                        //deleta dos outros servidores                      
                        
                        String nomeRemove = req.getNome();                        
                        String path = pasta + "/" + nomeRemove;
                        file = new File(path);
                        file.delete();                      
                        
                        break;
                    case 6:
                        //replica nos outros servidores
                        
                        FileName = req.getNome();
                        pasta += File.separator + FileName;                        
                       
                        lines = Arrays.asList(req.getConteudo());

                        try {
                            Files.write(Paths.get(pasta), lines, utf8);                            
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        
                        break;
                } 
            } else {
                try {
                    serversocket.close();
                    break;
                } catch (Exception e) {
                    System.out.println("Nao desconectei...");
                }
            }
        }
    }
    
    static boolean connect() {
        
        try {
            controlador_socket = serversocket.accept();
            return true;
        } catch (Exception e) {
            System.out.println("Erro de connect..." + e.getMessage());
            return false;
        }
    }  
}