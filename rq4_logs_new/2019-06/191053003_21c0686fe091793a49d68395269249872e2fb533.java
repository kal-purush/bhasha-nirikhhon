
package okno;

import java.awt.Graphics;
import java.awt.image.BufferedImage;

import javax.swing.JPanel;

public class Mapa extends JPanel {
    private int wysokoscMapy, szerokoscMapy, x, y;;
    private BufferedImage obraz;
   
    private double piksel;
    
    public Mapa(BufferedImage obraz){
        this.setSize(500, 500);                
         
        this.obraz = obraz;
        this.wysokoscMapy = obraz.getHeight();
        this.szerokoscMapy = obraz.getWidth();
        
        this.x = 1;
        this.y = 1;
        this.piksel = 500.0/ obraz.getHeight();
        
        if (obraz.getWidth() > obraz.getHeight()){
            
            this.piksel = 500.0 / obraz.getWidth();
            
            this.szerokoscMapy = 500;
            
            this.wysokoscMapy = (int)(obraz.getHeight() * piksel);
            
            this.y = (500-this.wysokoscMapy)/2;
            
        }
        else if (obraz.getWidth() <= obraz.getHeight()){
            
            this.piksel = 500.0 / obraz.getHeight();
            
            this.wysokoscMapy = 500;
            
            this.szerokoscMapy = (int)(obraz.getWidth() * piksel);
            
            this.x = (500 - this.szerokoscMapy)/2;
        } 
        
    }
    public void odrysuj(BufferedImage mapa){
        this.setObraz(mapa);
        this.repaint();
    }
    @Override
    public void paintComponent(Graphics grafika) {
        grafika.drawImage(obraz, this.x, this.y, this.szerokoscMapy, this.wysokoscMapy, this);
                                    //TODO obraz , polozenie, polozenie, wysokosc szerokosc,
                                      // moze zamiasr x,y  cyfry
    }
    public BufferedImage getObraz(){
        return obraz;
    }
    
    public double getPiksel(){
        return this.piksel;
    }
    
    
    public void setObraz(BufferedImage obraz){
        this.obraz = obraz;
    }
    public void setPiksel(double piksel){
        this.piksel = piksel;
    }
}