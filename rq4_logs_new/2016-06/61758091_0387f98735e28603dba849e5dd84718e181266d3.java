package imaage;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import javax.imageio.ImageIO;

/********
 * butt stuff
 * @param shit
 * @author <b><u>dave</b></u>
 *
 */
public class ConvertToCirclePicture
{
	public static void main(String[] args) throws FileNotFoundException
	{	
		//IMAGE SOURCE
		File originalImage = new File("C:\\Java\\Tutorial\\original.png");
		PrintWriter out = new PrintWriter("C:\\Java\\Tutorial\\out.txt");
	
		BufferedImage img = null;
		try {
			img = ImageIO.read(originalImage);
			
			double angle;
			double radius;
			int circleAngleElement; //abstract representation of the circular pixel grid
			int circleRadiusElement;
		
			int xRes = img.getWidth();
			int yRes = img.getHeight();
			int xCircleRes = 201;
			int yCircleRes = 32;
			
			int[][] circle = new int[xCircleRes][yCircleRes];	
			//populate the circle array with 0's
			for (int x = 0; x < xCircleRes; x++){
				for (int y = 0; y < yCircleRes; y++){
					circle[x][y] = 0;
				}
			}
			
			int boolCirclePlot[][] = new int[xCircleRes][yCircleRes];
			//populate the boolCirclePlot array with 0's
			for (int i = 0; i < xCircleRes; i++){
				for (int j = 0; j < yCircleRes; j++){
					boolCirclePlot[i][j] = 0;
				}
			}
			
			//int[][] origImageArray = new int[xRes][yRes]; 
			
			BufferedImage tempImage = new BufferedImage(img.getWidth(),
					img.getHeight(), BufferedImage.TYPE_INT_ARGB);
			
			for (int y=0; y < img.getHeight(); y++)
			{
				for (int x=0; x < img.getWidth(); x++)
				{
					//get RGP color on each pixel
					Color c = new Color(img.getRGB(x, y));
					int r = c.getRed();
					int g = c.getGreen();
					int b = c.getBlue();
					int a = c.getAlpha();
					
					angle = Math.atan2((yRes/2.0)-y, (xRes/2.0)-x) * (180/Math.PI) ;
					radius = Math.sqrt(Math.pow(xRes/2.0 - x, 2) + Math.pow(yRes/2.0 - y, 2));
					if (radius > 32)
						radius = 32.0;
					
					System.out.println("true angle: "+ angle);
					
				//QUADRANT SHIFTS
					
					if ( y > yRes/2.0)
					{
						angle = angle + 90; //assumed to be degrees
						
					}
					
					if (x < xRes/2.0 ) 
					{
						angle = angle + 90;
						if (y < xRes/2.0)
						{
							angle = angle + 180;
						}
					}
					
					System.out.println("radius: "+ radius);
					System.out.println("angle: "+ angle);

					int gr = (r + g + b) / 3;

					Color gColor = new Color(0,0,0,a);
					tempImage.setRGB(x, y, gColor.getRGB());
						
				//CIRCULAR GRID CHECK
					
					circleAngleElement = (int)(Math.floor(angle / (360.0/201.0)) ); //360/200 ~= 1.8 degrees
					System.out.println("circleAngleElement: "+ circleAngleElement);
					circleRadiusElement = (int)(Math.floor(radius) );
					System.out.println("circleRadiusElement: "+ circleRadiusElement);
					
					if(circleRadiusElement == 0){
						circleRadiusElement = 1;
					}
					
					circle[circleAngleElement][circleRadiusElement-1] =
							(circle[circleAngleElement][circleRadiusElement-1] + gr) / 2;
				}
			}
		
			//ImageIO.write(tempImage, "png", new File("C:\\Java\\Tutorial\\grayscale.png"));
			for (int y = 0; y < yCircleRes; y++){
				for (int x = 0; x < xCircleRes; x++){
					if(circle[x][y] < 150)
						boolCirclePlot[x][y] = 1;
					else
						boolCirclePlot[x][y] = 0;
				}
			}
			
			
			//SECTION TO DISPLAY CIRCULAR PICTURE
			for (int y = 0; y < yCircleRes; y++){
				for (int x = 0; x < xCircleRes; x++){
					out.print(boolCirclePlot[x][y]);
				}
			}
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		out.close();
	}
	
	
	
	
	
}