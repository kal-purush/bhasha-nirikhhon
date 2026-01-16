package ucsp.is2.model;

import java.util.ArrayList;
import java.io.File;
import javazoom.jlgui.basicplayer.BasicPlayer;
import javax.persistence.Entity;

@Entity

public class Reproductor {
    public BasicPlayer player;

    public Reproductor() {
        player = new BasicPlayer();
    }

public void funciones(String y){

}
    public void Play() throws Exception {
        player.play();
    }

    public void AbrirFichero(String ruta) throws Exception {
        player.open(new File(ruta));
    }

    public void Pausa() throws Exception {
        player.pause();
    }

    public void Continuar() throws Exception {
        player.resume();
    }

    public void Stop() throws Exception {
        player.stop();
    }
    public void reproducemp3 () throws Exception{
       try {
        Reproductor   mi_reproductor = new Reproductor();
            mi_reproductor.AbrirFichero("/Users/luis/Desktop/Musica”);
            mi_reproductor.Play();
        } catch (Exception ex) {
            System.out.println("Error: " + ex.getMessage());
        }
    }

    private int searchSong(String[] findSong, String songSearch) {
       int result = -1;
       int cant = 0;
       for(String songFounded : findSong){
           if(findSong[cant].equals(songSearch)){
               result = cant;
           }
           cant++;
       }
       return result;
    } 

    public static void main(String args[]) throws Exception{
     Reproductor y = new Reproductor();
     y.AbrirFichero("/Users/luis/Desktop/Musica/mirame.mp3");
     y.Play();
    }
}