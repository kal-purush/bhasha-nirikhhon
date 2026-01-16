import modelo.Serie;
import modelo.Ator;
import modelo.Episodio;
import util.Arquivo;
import index.ListaInvertida;
import index.Indexador;
import java.time.LocalDate;

public class Seeder {
    public static void main(String[] args) throws Exception {
        Arquivo<Serie> arqSeries = new Arquivo<>("Series", Serie.class.getConstructor());
        Arquivo<Ator> arqAtores = new Arquivo<>("Atores", Ator.class.getConstructor());
        Arquivo<Episodio> arqEpisodios = new Arquivo<>("Episodios", Episodio.class.getConstructor());

        ListaInvertida idxSeries = new ListaInvertida(10, "dados/Series/dicionario_series.dat", "dados/Series/blocos_series.dat");
        Indexador seederSeries = new Indexador(idxSeries);

        ListaInvertida idxAtores = new ListaInvertida(10, "dados/Atores/dicionario_atores.dat", "dados/Atores/blocos_atores.dat");
        Indexador seederAtores = new Indexador(idxAtores);

        ListaInvertida idxEpisodios = new ListaInvertida(10, "dados/Episodios/dicionario_episodios.dat", "dados/Episodios/blocos_episodios.dat");
        Indexador seederEpisodios = new Indexador(idxEpisodios);

        // Séries
        Serie s1 = new Serie("Breaking Bad", 2008, "Chemistry teacher turned meth producer.", "Netflix");
        int id1 = arqSeries.create(s1);
        seederSeries.indexarTitulo(id1, s1.getNome());

        Serie s2 = new Serie("Game of Thrones", 2011, "Noble families vie for control of the Iron Throne.", "HBO");
        int id2 = arqSeries.create(s2);
        seederSeries.indexarTitulo(id2, s2.getNome());

        Serie s3 = new Serie("Friends", 1994, "Friends navigate life in New York.", "NBC");
        int id3 = arqSeries.create(s3);
        seederSeries.indexarTitulo(id3, s3.getNome());

        // Atores
        Ator a1 = new Ator("Bryan Cranston");
        int aid1 = arqAtores.create(a1);
        seederAtores.indexarTitulo(aid1, a1.getNome());

        Ator a2 = new Ator("Aaron Paul");
        int aid2 = arqAtores.create(a2);
        seederAtores.indexarTitulo(aid2, a2.getNome());

        Ator a3 = new Ator("Emilia Clarke");
        int aid3 = arqAtores.create(a3);
        seederAtores.indexarTitulo(aid3, a3.getNome());

        Ator a4 = new Ator("Peter Dinklage");
        int aid4 = arqAtores.create(a4);
        seederAtores.indexarTitulo(aid4, a4.getNome());

        Ator a5 = new Ator("Jennifer Aniston");
        int aid5 = arqAtores.create(a5);
        seederAtores.indexarTitulo(aid5, a5.getNome());

        // Episódios
        Episodio ep1 = new Episodio(id1, "Pilot", 1, LocalDate.of(2008, 1, 20), 58, "Initial episode");
        int eid1 = arqEpisodios.create(ep1);
        seederEpisodios.indexarTitulo(eid1, ep1.getNome());

        Episodio ep2 = new Episodio(id1, "Cat's in the Bag...", 1, LocalDate.of(2008, 1, 27), 48, "Episode 2");
        int eid2 = arqEpisodios.create(ep2);
        seederEpisodios.indexarTitulo(eid2, ep2.getNome());

        Episodio ep3 = new Episodio(id2, "Winter Is Coming", 1, LocalDate.of(2011, 4, 17), 62, "GoT Pilot");
        int eid3 = arqEpisodios.create(ep3);
        seederEpisodios.indexarTitulo(eid3, ep3.getNome());

        Episodio ep4 = new Episodio(id3, "The One Where Monica Gets a Roommate", 1, LocalDate.of(1994, 9, 22), 22, "Friends Pilot");
        int eid4 = arqEpisodios.create(ep4);
        seederEpisodios.indexarTitulo(eid4, ep4.getNome());

        System.out.println("Seed concluído com sucesso.");
    }
} 