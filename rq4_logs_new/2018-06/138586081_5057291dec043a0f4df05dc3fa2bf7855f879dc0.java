import java.util.Random;

public class Aufgabe7 {


    public static void main(String[] args) {
        String fileName = "Test1_list";

        /*
        Playlist1 playList = new Playlist1();

        Song newSong = null;
        Random r = new Random();
        for (int i = 0; i<=50; i++) {
            newSong = new Song(((char) (r.nextInt(26) + 'a')) + "-"  + ((char) (r.nextInt(26) + 'a')) + "Song" + i,
                    "Tenacious D", (100 * i % 1000) + 10 * (i / 10));
            playList.add(newSong);
        }
        */


        System.out.println("\nTest read Playlist");
        Playlist1 playlist2 = new Playlist1(fileName);
        playlist2.print();


        System.out.println("\nTest save Playlist");
        playlist2.save(fileName);


    }
}