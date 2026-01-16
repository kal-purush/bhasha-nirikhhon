/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cosechaoperacionesv1.clases;

import java.io.File;
import javafx.scene.media.Media;
import javafx.scene.media.MediaPlayer;

/**
 *
 * @author acastroc
 */
public class Sonidos {
    File fondo=new File("la-atmosfera_4.mp3");/**Se crea el objeto file con la ruta del sonido*/
    String sfondo="file:///" +fondo.getAbsolutePath();/** ruta ejecutable*/
    
    MediaPlayer mediaplayer;
    
    public void SonidoFondo(){
        sfondo =sfondo.replace("\\", "/");
        Media sonidoFile = new Media(sfondo);
        mediaplayer =new MediaPlayer(sonidoFile);/** Variable que contien la URL*/
        mediaplayer.play();
    }
}