 
package com;

import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.embed.swing.SwingFXUtils;
import javafx.scene.image.Image;
import javafx.scene.image.WritableImage;
import javafx.scene.layout.Background;
import javafx.scene.layout.BackgroundImage;
import javafx.scene.layout.BackgroundPosition;
import javafx.scene.layout.BackgroundRepeat;
import javafx.scene.layout.BackgroundSize;
import javafx.scene.layout.Pane;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javax.imageio.ImageIO;

 
public class Archivo {
    
    
    private FileChooser file;
    private Stage stage;
    private Pane canvas;
    private File archivo;
    
    
    
    public Archivo(Pane canvas)
    {
        this.canvas = canvas;
    }
    
    public void save()
    {
        stage= new Stage();
             
        file = new FileChooser();
         FileChooser.ExtensionFilter extFilter = 
                        new FileChooser.ExtensionFilter("png files (*.png)", "*.png");
                file.getExtensionFilters().add(extFilter);
                
                  
         File archivo = file.showSaveDialog(stage);
         
         if (archivo!=null) {
             
             try
             {
                
                 WritableImage writableImage = new WritableImage((int)canvas.getWidth(),(int)canvas.getHeight());
                
                 Image s  = canvas.snapshot(null,writableImage);
                            
                 BufferedImage  Img = SwingFXUtils.fromFXImage(s, null);
                        
                 ImageIO.write(Img, "png", archivo);
             
                 
             }
             catch(IOException e){}
            
        }
    }
    
    
    public void open()
    {
        
        stage = new Stage();
  
        file = new FileChooser();
        
        archivo = file.showOpenDialog(stage);
        
        if (archivo!=null) {
            
            try {
               
                BufferedImage img  = ImageIO.read(archivo);
                Image image = SwingFXUtils.toFXImage(img,null);
                
                
                
                 BackgroundImage myBI = new BackgroundImage(image,
                 BackgroundRepeat.REPEAT, BackgroundRepeat.NO_REPEAT, BackgroundPosition.DEFAULT,
          BackgroundSize.DEFAULT);
                 
                 
                 canvas.getChildren().clear();
                 canvas.setBackground(new Background(myBI));
                
                             
            } catch (IOException ex) {
                Logger.getLogger(Archivo.class.getName()).log(Level.SEVERE, null, ex);
            }
              
               
        }
    }
    
    
}