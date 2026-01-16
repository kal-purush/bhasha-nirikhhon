package application;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import net.sf.image4j.codec.ico.ICODecoder;
import javafx.embed.swing.SwingFXUtils;
import javafx.scene.control.ListCell;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.image.WritableImage;

public class ListViewItem extends ListCell<String>{
    @Override
    public void updateItem(String item, boolean empty) {
    	
    	//System.out.println(item);
    	if(item != null){
	    	ImageView iconView = new ImageView(getIconImage(toFaviURL(item)));
	    	setGraphic(iconView);
    	}
    }
	public Image getIconImage(String url){
		//url = "http://www.google.com/s2/favicons?domain=www.yahoo.co.jp";
		InputStream input = null;
		try {
			//System.out.println("test2");
			URL favURL = new URL(url);
			input = favURL.openStream();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (input == null){
			//System.out.println("test3");
			input = getClass().getResourceAsStream("default.ico");
		}
		
		if (input != null){
			//System.out.println("test4");
			try {
				BufferedImage img = null;
				List<BufferedImage> imgs = ICODecoder.read(input);
				img = imgs != null ? imgs.size() > 0 ? imgs.get(0) : null : null;
				img = resize(img,16,16);
				WritableImage imfx = SwingFXUtils.toFXImage(img, null);
				if(imfx == null){
				}
				return imfx;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}else{
			//System.out.println("test5");
			return null;
		}
		return null;
		
		
	}
	public String toFaviURL(String url){
		int s = url.indexOf(":")+3;
		int e = url.indexOf("/",s);
		String faviURL = url.substring(0,e) + "/favicon.ico";
		return faviURL;
	}
	public static BufferedImage resize(BufferedImage img, int newW, int newH) { 
	    java.awt.Image tmp = img.getScaledInstance(newW, newH, java.awt.Image.SCALE_SMOOTH);
	    BufferedImage dimg = new BufferedImage(newW, newH, BufferedImage.TYPE_INT_ARGB);

	    Graphics2D g2d = dimg.createGraphics();
	    g2d.drawImage(tmp, 0, 0, null);
	    g2d.dispose();

	    return dimg;
	}
}