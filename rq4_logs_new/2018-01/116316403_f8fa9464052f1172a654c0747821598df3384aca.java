import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;


public class ImageClass {

	private BufferedImage image = null;
	private BufferedImage imageCopy = null;
	private BufferedImage grayImage = null;
	private File f = null;
	
	//	Constructor
	public ImageClass() {
		super();
	}
	
	//	Getter
	public BufferedImage getImage() {
		return this.image;
	}

	//	Read image
	public void readImage(String path) {
		try {
			this.f = new File(path);
			this.image = ImageIO.read(this.f);
			this.grayImage = ImageIO.read(this.f);
			this.imageCopy = ImageIO.read(this.f);	
		} catch(IOException e) {
			System.out.println("Error: " + e);
		}
	}
	
	// Write image
	public void writeImage(String newPath) {
		try{
			  f = new File(newPath);  
			  ImageIO.write(image, "jpg", f);
			  System.out.println("Writing complete.");
		}catch(IOException e) {
		      System.out.println("Error: " + e);
		}
		
	}
	
	// Brightness adjustment
	public void BRIGHTNESS(int B) {
		// Get image width and height
		int width = this.imageCopy.getWidth();
		int height = this.imageCopy.getHeight();
		
		for(int y = 0; y < height; y++) {
			for(int x = 0; x < width; x++){
				
				int p = this.imageCopy.getRGB(x, y);
				
				int r = (p>>16)&0xff;
				int g = (p>>8)&0xff;
				int b = p&0xff;
				
				//	Calculate contrast correction
				r = this.Truncate((int) (r + B));
				g = this.Truncate((int) (g + B));
				b = this.Truncate((int) (b + B));
	
				p = (r << 16) | (g << 8) | b;
				this.image.setRGB(x, y, p);
			}
		}
	}
	
	//	Convert to Gray
	public void FILTER_GRAY() {
		// Get image width and height
		int width = this.imageCopy.getWidth();
		int height = this.imageCopy.getHeight();
		
		for(int y = 0; y < height; y++) {
			for(int x = 0; x < width; x++){
				
				int p = this.imageCopy.getRGB(x, y);
				
				int r = (p>>16)&0xff;
				int g = (p>>8)&0xff;
				int b = p&0xff;
				
				//	Find average for (x, y) pixel
				int avg = (r+g+b)/3;
				
				p = (avg << 16) | (avg << 8) | avg;
				this.image.setRGB(x, y, p);
			}
		}
	}
	
	// Contrast adjustment
	public void CONTRAST(int C) {
		// Contrast correction factor
		float factor = (float)(259 *(C + 255)) / (255 * (259 - C));
		
		// Get image width and height
		int width = this.imageCopy.getWidth();
		int height = this.imageCopy.getHeight();
		
		for(int y = 0; y < height; y++) {
			for(int x = 0; x < width; x++){
				
				int p = this.imageCopy.getRGB(x, y);
				
				int r = (p>>16)&0xff;
				int g = (p>>8)&0xff;
				int b = p&0xff;
				
				//	Calculate contrast correction
				r = this.Truncate((int) (factor * (r - 128) + 128));
				g = this.Truncate((int) (factor * (g - 128) + 128));
				b = this.Truncate((int) (factor * (b - 128) + 128));
	
				p = (r << 16) | (g << 8) | b;
				this.image.setRGB(x, y, p);
			}
		}
		
	}
	
	//	Black & White adjustment
	public void BLACK_WHITE(int RED, int YELLOW, int GREEN, int CYAN, int BLUE, int MAGENTA) {
		
		this.FILTER_GRAY_OF(this.grayImage);
		int RED_COPY = RED;
		int YELLOW_COPY = YELLOW;
		int GREEN_COPY = GREEN;
		int CYAN_COPY = CYAN;
		int BLUE_COPY = BLUE;
		int MAGENTA_COPY = MAGENTA;
		
		// Get image width and height
		int width = this.imageCopy.getWidth();
		int height = this.imageCopy.getHeight();
		
		for(int y = 0; y < height; y++) {
			for(int x = 0; x < width; x++){
				
				RED = RED_COPY;
				YELLOW = YELLOW_COPY;
				GREEN = GREEN_COPY;
				CYAN = CYAN_COPY;
				BLUE = BLUE_COPY;
				MAGENTA = MAGENTA_COPY;
				
				int p = this.imageCopy.getRGB(x, y);

				int r = (p>>16)&0xff;
				int g = (p>>8)&0xff;
				int b = p&0xff;
				float c = 1.0f - r / 255.0f;
				float m = 1.0f - g / 255.0f;
				float yy = 1.0f - b / 255.0f;
				float k = Math.min(c, Math.min(m, yy));
				if(k >= 1.0f) {
					c = 0;
					m = 0;
					yy = 0;
				} else {
					float s = 1.0f - k;
					c = (c - k) / s;
					m = (m - k) / s;
					yy = (yy - k) / s;
				}
				
				
				float HSV[] = new float[3];
				
				Color.RGBtoHSB(r, g, b, HSV);
				
				RED = (int) (8 * RED * HSV[1] * HSV[2] * HSV[2] * (m - c));
				YELLOW = (int) (3 * YELLOW * HSV[1] * HSV[2] * (yy - m) * r/255.0);
				GREEN = (int) (40 * GREEN * HSV[1] * HSV[2] * (yy - m) * (c - m));
				CYAN = (int) (10 * CYAN * HSV[1] * HSV[2] * (c - m) * g/255.0);
				BLUE = (int) (70 * BLUE * HSV[1]  * HSV[2] * (b/255.0 - g/255.0) * (b/255.0 - g/255.0));
				MAGENTA = (int) (10 * MAGENTA * HSV[1] * HSV[1] * HSV[2] * HSV[2] * (k*k));
				
				//	RED
				if(r > g && r > b) {
					this.BRIGHTNESS_AT(RED, this.grayImage, x, y);
				}
				// YELLOW
				if(yy > c && yy > m) {
					this.BRIGHTNESS_AT(YELLOW, this.grayImage, x, y);
				}
				// GREEN
				if(g > r && g > b) {
					this.BRIGHTNESS_AT(GREEN, this.grayImage, x, y);
				}
				// CYAN
				if(c > yy && c > m) {
					this.BRIGHTNESS_AT(CYAN, this.grayImage, x, y);
				}
				// BLUE
				if(b > r && b > g) {
					this.BRIGHTNESS_AT(BLUE, this.grayImage, x, y);
				} 
				// MAGENTA 
				if(m > yy && m > c) {
					this.BRIGHTNESS_AT(MAGENTA, this.grayImage, x, y);
				}
			}
		}
		
		this.image = this.grayImage;
	}
	
	//	Functions
	private int Truncate(int value) {
		if(value < 0) return 0;
		if(value > 255) return 255;
		return value;
	}
	
	//	Convert to Gray
	private BufferedImage FILTER_GRAY_OF(BufferedImage img) {
		// Get image width and height
		int width = this.imageCopy.getWidth();
		int height = this.imageCopy.getHeight();
		
		for(int y = 0; y < height; y++) {
			for(int x = 0; x < width; x++){
				
				int p = this.imageCopy.getRGB(x, y);
				
				int r = (p>>16)&0xff;
				int g = (p>>8)&0xff;
				int b = p&0xff;
				
				//	Find average for (x, y) pixel
				int avg = (r+g+b)/3;
				
				p = (avg << 16) | (avg << 8) | avg;
				img.setRGB(x, y, p);
			}
		}
		return img;
	}
	
	//	Change pixel brightness
	private void BRIGHTNESS_AT(int B, BufferedImage grayImage, int x, int y) {
		// Get image width and height

		int p = grayImage.getRGB(x, y);
		
		int r = (p>>16)&0xff;
		int g = (p>>8)&0xff;
		int b = p&0xff;
		
		//	Add brightness
		r = this.Truncate((int) (r + B));
		g = this.Truncate((int) (g + B));
		b = this.Truncate((int) (b + B));

		p = (r << 16) | (g << 8) | b;
		grayImage.setRGB(x, y, p);
	}
}