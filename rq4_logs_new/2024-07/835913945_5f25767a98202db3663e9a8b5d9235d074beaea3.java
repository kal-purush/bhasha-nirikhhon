

public class Filmes extends Publicacao{
    public String diretor;
    public String atorP;
    public int duracao;
    public String sinopse;
  
    public Filmes(String titulo , String autor , int ano , String genero , String editora , int qntd, String diretor, String atorP, int duracao, String sinopse){
        super(titulo, autor, ano, genero, editora, qntd);
        this.diretor = diretor;
        this.atorP = atorP;
        this.duracao = duracao;
        this.sinopse = sinopse;
    }


    public String getDiretor(){
        return diretor;
    }
    public String getAtorP(){
        return atorP;
    }
    public int getDuracao(){
        return duracao;
    }
    public String getSinopse(){
        return sinopse;
    }

    public void imprimeDados(){
        System.out.println("Titulo: "+titulo+ "\nAutor: "+autor+ "\nAno de publicacao: "+ano+ "\nGenero do livro: "+genero+"\nEditora: "+editora+"\nQuantidade disponivel: "+qntd+ "Diretor:"+diretor+"Ator principal:"+atorP+"Duração:"+duracao+"Sinopse:"+sinopse);
       
      }

 

}